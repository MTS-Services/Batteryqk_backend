import prisma from '../utils/prismaClient.js';
import { recordAuditLog } from '../utils/auditLogHandler.js';
import { AuditLogAction } from '@prisma/client';
import { createClient } from "redis";
import * as deepl from "deepl-node";
import { sendMail } from '../utils/mailer.js';

// --- DeepL Configuration ---
const DEEPL_AUTH_KEY = process.env.DEEPL_AUTH_KEY || "YOUR_DEEPL_AUTH_KEY_HERE";
if (DEEPL_AUTH_KEY === "YOUR_DEEPL_AUTH_KEY_HERE") {
    console.warn("DeepL Auth Key is a placeholder. AR translations may not work. Please configure process.env.DEEPL_AUTH_KEY.");
}
const deeplClient = DEEPL_AUTH_KEY !== "YOUR_DEEPL_AUTH_KEY_HERE" ? new deepl.Translator(DEEPL_AUTH_KEY) : null;

// --- Redis Configuration ---
const REDIS_URL = process.env.REDIS_URL || "redis://default:YOUR_REDIS_PASSWORD@YOUR_REDIS_HOST:PORT";
const AR_CACHE_EXPIRATION = 365 * 24 * 60 * 60; // 365 days in seconds

const redisClient = createClient({
    url: REDIS_URL,
    socket: {
        reconnectStrategy: (retries) => {
            if (retries >= 3) return new Error('Max reconnection retries reached.');
            return Math.min(retries * 200, 5000);
        },
    },
});

redisClient.on('error', (err) => console.error('Redis: Review Cache - Error ->', err.message));
(async () => {
    try {
        await redisClient.connect();
        console.log('Redis: Review Cache - Connected successfully.');
    } catch (err) {
        console.error('Redis: Review Cache - Could not connect ->', err.message);
    }
})();

const cacheKeys = {
    reviewAr: (reviewId) => `review:${reviewId}:ar`,
    userReviewsAr: (uid) => `user:${uid}:reviews:ar`,
    userBookingsAr: (uid) => `user:${uid}:bookings:ar`,
    listingReviewsAr: (listingId) => `listing:${listingId}:reviews:ar`,
    allReviewsAr: (filterHash = '') => `reviews:all${filterHash}:ar`,
    listingAr: (listingId) => `listing:${listingId}:ar`,
    bookingAr: (bookingId) => `booking:${bookingId}:ar`,
};

// --- Helper Functions ---
async function translateText(text, targetLang, sourceLang = null) {
    if (!deeplClient) {
        console.warn("DeepL client is not initialized.");
        return text;
    }

    if (!text || typeof text !== 'string') {
        return text;
    }

    try {
        const result = await deeplClient.translateText(text, sourceLang, targetLang);
        console.log(`Translated: "${text}" => "${result.text}"`);
        return result.text;
    } catch (error) {
        console.error(`DeepL Translation error: ${error.message}`);
        return text;
    }
}

async function translateArrayFields(arr, targetLang, sourceLang = null) {
    if (!arr || !Array.isArray(arr)) return arr;
    return await Promise.all(arr.map(item => translateText(item, targetLang, sourceLang)));
}

async function translateListingFields(listing, targetLang, sourceLang = null) {
    if (!listing) return listing;

    const translatedListing = { ...listing };

    // Translate basic text fields
    if (listing.name)
        translatedListing.name = await translateText(listing.name, targetLang, sourceLang);

    if (listing.description)
        translatedListing.description = await translateText(listing.description, targetLang, sourceLang);

    // Translate array fields
    const arrayFields = ['agegroup', 'location', 'facilities', 'operatingHours'];
    for (const field of arrayFields) {
        if (Array.isArray(listing[field]))
            translatedListing[field] = await translateArrayFields(listing[field], targetLang, sourceLang);
    }

    // Translate categories
    const categoryFields = ['selectedMainCategories', 'selectedSubCategories', 'selectedSpecificItems'];
    for (const field of categoryFields) {
        if (Array.isArray(listing[field])) {
            translatedListing[field] = await Promise.all(
                listing[field].map(async (item) => ({
                    ...item,
                    name: await translateText(item.name, targetLang, sourceLang)
                }))
            );
        }
    }

    // Translate reviews
    if (Array.isArray(listing.reviews)) {
        translatedListing.reviews = await Promise.all(
            listing.reviews.map(async (review) => ({
                ...review,
                comment: await translateText(review.comment, targetLang, sourceLang),
                status: await translateText(review.status, targetLang, sourceLang),
                user:  await translateText(review.user.fname, targetLang, sourceLang) + ' ' + await translateText(review.user.lname, targetLang, sourceLang),
            }))
        );
    }

    // Translate bookings
    if (Array.isArray(listing.bookings)) {
        translatedListing.bookings = await Promise.all(
            listing.bookings.map(async (booking) => ({
                ...booking,
                additionalNote: await translateText(booking.additionalNote, targetLang, sourceLang),
                ageGroup: await translateText(booking.ageGroup, targetLang, sourceLang),
                status: await translateText(booking.status, targetLang, sourceLang),
                booking_hours: booking.booking_hours
                    ? await translateText(booking.booking_hours, targetLang, sourceLang)
                    : null,
                user: await translateText(booking.user.fname, targetLang, sourceLang) + ' ' + await translateText(booking.user.lname, targetLang, sourceLang),
                paymentMethod: await translateText(booking.paymentMethod, targetLang, sourceLang)
            }))
        );
    }
    
    return translatedListing;
}

function createFilterHash(filters) {
    const sortedFilters = Object.keys(filters).sort().reduce((result, key) => {
        result[key] = filters[key];
        return result;
    }, {});
    return JSON.stringify(sortedFilters);
}

// --- Review Service ---
const reviewService = {

    // 1. Create Review
    async createReview(data, userUid, lang = 'en', reqDetails = {}) {
        try {
            const { listingId, bookingId, rating, comment } = data;

            const user = await prisma.user.findUnique({ where: { uid: userUid } });
            if (!user) throw new Error('User not found');

            const listing = await prisma.listing.findUnique({ where: { id: listingId } });
            if (!listing) throw new Error('Listing not found');

            // Verify booking exists and belongs to user if bookingId is provided
            let booking = null;
            if (bookingId) {
                booking = await prisma.booking.findUnique({ 
                    where: { id: bookingId },
                    include: { user: true, review: true }
                });
                
                if (!booking || booking.userId !== user.id) {
                    throw new Error('Booking not found or does not belong to user');
                }

                // Check if booking status is appropriate for review
                if (booking.status === 'PENDING') {
                    throw new Error('Cannot review a pending booking. Please wait for booking confirmation.');
                }

                if (booking.status === 'CANCELLED') {
                    throw new Error('Cannot review a cancelled booking.');
                }

                // Check if booking payment method is PAID
                if (booking.paymentMethod !== 'PAID') {
                    throw new Error('You can only review bookings that have been paid for.');
                }

                // Check if booking already has a review
                if (booking.review) {
                    throw new Error('This booking already has a review.');
                }
            }

            let dataForDb = { comment };
            if (lang === 'ar' && deeplClient) {
                dataForDb.comment = await translateText(comment, 'EN-US', 'AR');
            }

            const review = await prisma.review.create({
                data: {
                    userId: user.id,
                    listingId: listingId,
                    rating: parseInt(rating),
                    comment: dataForDb.comment,
                    status: 'PENDING',
                },
                include: { 
                    user: { select: { fname: true, lname: true, uid: true } }, 
                    listing: { select: { name: true, id: true } } 
                }
            });

            // Update booking with review reference if bookingId provided
            if (bookingId) {
                await prisma.booking.update({
                    where: { id: bookingId },
                    data: { review_id: review.id }
                });
            }

            const immediateResponse = lang === 'ar' ? {
                message: 'تم إرسال تقييمك بنجاح وهو الآن قيد المراجعة.',
                reviewId: review.id,
                listingName: await translateText(listing.name, 'AR', 'EN'),
                status: await translateText('PENDING', 'AR', 'EN'),
                rating: rating
            } : {
                message: 'Review submitted successfully and is now pending approval.',
                reviewId: review.id,
                listingName: listing.name,
                status: 'PENDING',
                rating: rating
            };

            setImmediate(async () => {
                try {
                    // Send notification to user
                    await prisma.notification.create({
                        data: {
                            userId: user.id,
                            title: 'Review Submitted',
                            message: `Your review for ${listing.name} has been submitted and is pending approval.`,
                            type: 'GENERAL',
                            entityId: review.id.toString(),
                            entityType: 'Review'
                        }
                    });

                    // Send email to user
                    const userSubject = lang === 'ar' ? 'تم استلام تقييمك' : 'Review Received';
                    const userMessage = lang === 'ar' ? 
                        `مرحباً ${user.fname || 'العميل'},\n\nشكراً لك على تقييمك لـ: ${listing.name}. تقييمك الآن قيد المراجعة وسيتم نشره قريباً.` :
                        `Hello ${user.fname || 'Customer'},\n\nThank you for your review of: ${listing.name}. Your review is now pending approval and will be published soon.`;
                    
                    await sendMail(user.email, userSubject, userMessage, lang, { 
                        name: user.fname || 'Customer', 
                        listingName: listing.name 
                    });

                    // Send email to admin
                    const adminMessage = `Hello Admin,\n\nA new review requires your approval.\n\nListing: ${listing.name}\nReviewer: ${user.fname} ${user.lname} (${user.email})\nRating: ${rating}/5\nComment: ${comment || 'No comment'}`;
                    await sendMail(process.env.EMAIL_USER, 'New Review - Approval Required', adminMessage, 'en', { 
                        reviewerName: `${user.fname} ${user.lname}`, 
                        listingName: listing.name 
                    });

                    // Clear relevant caches
                    if (redisClient.isReady) {
                        const keysToDel = [
                            cacheKeys.userReviewsAr(user.uid),
                            cacheKeys.listingReviewsAr(listingId),
                            cacheKeys.listingAr(listingId)
                        ];
                        const allReviewsKeys = await redisClient.keys(cacheKeys.allReviewsAr('*'));
                        if (allReviewsKeys.length) keysToDel.push(...allReviewsKeys);
                        if (keysToDel.length > 0) await redisClient.del(keysToDel);
                    }

                    recordAuditLog(AuditLogAction.GENERAL_CREATE, {
                        userId: user.id,
                        entityName: 'Review',
                        entityId: review.id.toString(),
                        newValues: review,
                        description: `Review created for listing '${listing.name}' by ${user.email}.`,
                        ipAddress: reqDetails.ipAddress,
                        userAgent: reqDetails.userAgent,
                    });

                } catch (bgError) {
                    console.error(`Background task error for review ${review.id}:`, bgError);
                }
            });

            return immediateResponse;
        } catch (error) {
            console.error(`Failed to create review: ${error.message}`);
            throw new Error(`Failed to create review: ${error.message}`);
        }
    },

    // 2. Get All Reviews with Filters
    async getAllReviews(filters = {}, lang = 'en') {
        try {
            const { page = 1, limit = 10, ...restFilters } = filters;
            const pageNum = parseInt(page);
            const limitNum = parseInt(limit);
            const skip = (pageNum - 1) * limitNum;
            const filterHash = createFilterHash({ ...restFilters, page: pageNum, limit: limitNum });
            const cacheKey = cacheKeys.allReviewsAr(filterHash);

            // Check cache first
            if (lang === 'ar' && redisClient.isReady) {
                const cachedData = await redisClient.get(cacheKey);
                if (cachedData) {
                    const parsed = JSON.parse(cachedData);
                    console.log('Returning cached reviews:', parsed.reviews.length);
                    return parsed;
                }
            }
            
            // Build where clause from filters
            const whereClause = {
                ...(restFilters.status && { status: restFilters.status }),
                ...(restFilters.listingId && { listingId: parseInt(restFilters.listingId) }),
                ...(restFilters.rating && { rating: parseInt(restFilters.rating) }),
            };

            // Get data from database
            const [reviews, total] = await prisma.$transaction([
                prisma.review.findMany({
                    where: whereClause,
                    include: { 
                        user: { select: { uid: true, fname: true, lname: true } }, 
                        listing: { select: { name: true, id: true } },
                        booking: { select: { id: true, bookingDate: true } }
                    },
                    orderBy: { createdAt: 'desc' },
                    skip,
                    take: limitNum
                }),
                prisma.review.count({ where: whereClause })
            ]);

            console.log('Database reviews found:', reviews.length);

            let result = {
                reviews,
                pagination: { total, page: pageNum, limit: limitNum, totalPages: Math.ceil(total / limitNum) }
            };

            // Translate if Arabic and deeplClient is available
            if (lang === 'ar' && deeplClient) {
                result.reviews = await Promise.all(reviews.map(async (review) => {
                    const translatedReview = { ...review };
                    
                    // Translate review fields
                    if (review.comment) {
                        translatedReview.comment = await translateText(review.comment, 'AR', 'EN');
                    }
                    if (review.status) {
                        translatedReview.status = await translateText(review.status, 'AR', 'EN');
                    }
                    
                    // Translate listing name if present
                    if (review.listing && review.listing.name) {
                        translatedReview.listing = {
                            ...review.listing,
                            name: await translateText(review.listing.name, 'AR', 'EN')
                        };
                    }
                    
                    return translatedReview;
                }));
                
                // Cache translated results
                if (redisClient.isReady) await redisClient.setEx(cacheKey, AR_CACHE_EXPIRATION, JSON.stringify(result));
            }

            return result;
        } catch (error) {
            console.error(`Failed to get all reviews: ${error.message}`);
            throw new Error(`Failed to get all reviews: ${error.message}`);
        }
    },

    // 3. Get Review by ID
    async getReviewById(id, lang = 'en') {
        try {
            const reviewId = parseInt(id);
            const cacheKey = cacheKeys.reviewAr(reviewId);

            if (lang === 'ar' && redisClient.isReady) {
                const cachedReview = await redisClient.get(cacheKey);
                if (cachedReview) return JSON.parse(cachedReview);
            }

            const review = await prisma.review.findUnique({
                where: { id: reviewId },
                include: { 
                    user: { select: { uid: true, fname: true, lname: true } }, 
                    listing: { select: { name: true, id: true } },
                    booking: { select: { id: true, bookingDate: true } }
                }
            });

            if (!review) return null;

            if (lang === 'ar' && deeplClient) {
                const translatedReview = { ...review };
                
                // Translate review fields
                if (review.comment) {
                    translatedReview.comment = await translateText(review.comment, 'AR', 'EN');
                }
                if (review.status) {
                    translatedReview.status = await translateText(review.status, 'AR', 'EN');
                }
                
                // Translate listing name if present
                if (review.listing && review.listing.name) {
                    translatedReview.listing = {
                        ...review.listing,
                        name: await translateText(review.listing.name, 'AR', 'EN')
                    };
                }
                
                if (redisClient.isReady) await redisClient.setEx(cacheKey, AR_CACHE_EXPIRATION, JSON.stringify(translatedReview));
                return translatedReview;
            }

            return review;
        } catch (error) {
            console.error(`Failed to get review by ID ${id}: ${error.message}`);
            throw new Error(`Failed to get review by ID ${id}: ${error.message}`);
        }
    },

    // 4. Get Reviews by User UID
    async getReviewsByUserUid(uid, lang = 'en') {
        try {
            const cacheKey = cacheKeys.userReviewsAr(uid);

            if (lang === 'ar' && redisClient.isReady) {
                const cachedReviews = await redisClient.get(cacheKey);
                if (cachedReviews) return JSON.parse(cachedReviews);
            }

            const reviews = await prisma.review.findMany({
                where: { user: { uid: uid } },
                include: { 
                    listing: { select: { name: true, id: true } },
                    booking: { select: { id: true, bookingDate: true } }
                },
                orderBy: { createdAt: 'desc' }
            });

            if (lang === 'ar' && deeplClient) {
                const translatedReviews = await Promise.all(reviews.map(async (review) => {
                    const translatedReview = { ...review };
                    
                    // Translate review fields
                    if (review.comment) {
                        translatedReview.comment = await translateText(review.comment, 'AR', 'EN');
                    }
                    if (review.status) {
                        translatedReview.status = await translateText(review.status, 'AR', 'EN');
                    }
                    
                    // Translate listing name if present
                    if (review.listing && review.listing.name) {
                        translatedReview.listing = {
                            ...review.listing,
                            name: await translateText(review.listing.name, 'AR', 'EN')
                        };
                    }
                    
                    return translatedReview;
                }));
                
                if (redisClient.isReady) await redisClient.setEx(cacheKey, AR_CACHE_EXPIRATION, JSON.stringify(translatedReviews));
                return translatedReviews;
            }

            return reviews;
        } catch (error) {
            console.error(`Failed to get reviews for user ${uid}: ${error.message}`);
            throw new Error(`Failed to get reviews for user ${uid}: ${error.message}`);
        }
    },

    // 5. Update Review (Only rating and comment can be edited by users)
    async updateReview(id, data, userUid, lang = 'en', reqDetails = {}) {
        try {
            const reviewId = parseInt(id);
            const currentReview = await prisma.review.findUnique({
                where: { id: reviewId },
                include: { 
                    user: true, 
                    listing: { select: { name: true, id: true } } 
                }
            });
            
            if (!currentReview) throw new Error('Review not found');

            // Check if user owns the review
            if (userUid && currentReview.user.uid !== userUid) {
                throw new Error('You can only edit your own reviews');
            }

            // Only allow editing rating and comment for regular users
            let updateData = {};
            if (data.rating !== undefined) updateData.rating = parseInt(data.rating);
            if (data.comment !== undefined) {
                if (lang === 'ar' && deeplClient) {
                    updateData.comment = await translateText(data.comment, 'EN-US', 'AR');
                } else {
                    updateData.comment = data.comment;
                }
            }

            // Admin can update status
            if (data.status !== undefined && !userUid) {
                updateData.status = data.status.toUpperCase();
            }

            const updatedReview = await prisma.review.update({
                where: { id: reviewId },
                data: updateData,
                include: { 
                    user: { select: { uid: true, fname: true, lname: true } }, 
                    listing: { select: { name: true, id: true } },
                    booking: { select: { id: true, bookingDate: true } }
                }
            });

            setImmediate(async () => {
                try {
                    // Send notification for status updates (admin action)
                    if (data.status && data.status !== currentReview.status) {
                        const statusMessage = data.status === 'ACCEPTED' ? 
                            `Your review for ${currentReview.listing.name} has been approved and is now live.` :
                            `Your review for ${currentReview.listing.name} has been ${data.status.toLowerCase()}.`;
                        
                        await prisma.notification.create({
                            data: {
                                userId: currentReview.user.id,
                                title: 'Review Status Updated',
                                message: statusMessage,
                                type: 'GENERAL',
                                entityId: reviewId.toString(),
                                entityType: 'Review'
                            }
                        });

                        // Send email notification
                        const emailSubject = 'Review Status Update';
                        const emailMessage = `Hello ${currentReview.user.fname || 'Customer'},\n\n${statusMessage}\n\nThank you for your feedback.`;
                        await sendMail(currentReview.user.email, emailSubject, emailMessage, 'en', {
                            name: currentReview.user.fname || 'Customer',
                            listingName: currentReview.listing.name,
                            status: data.status
                        });
                    }

                    // Clear relevant caches
                    if (redisClient.isReady) {
                        const keysToDel = [
                            cacheKeys.reviewAr(reviewId),
                            cacheKeys.userReviewsAr(currentReview.user.uid),
                            cacheKeys.listingReviewsAr(currentReview.listingId),
                            cacheKeys.listingAr(currentReview.listingId)
                        ];
                        const allReviewsKeys = await redisClient.keys(cacheKeys.allReviewsAr('*'));
                        if (allReviewsKeys.length) keysToDel.push(...allReviewsKeys);
                        if (keysToDel.length > 0) await redisClient.del(keysToDel);

                        // Update listing cache if status changed to accepted/rejected
                        if (data.status && data.status !== currentReview.status) {
                            const listingCacheKey = cacheKeys.listingAr(currentReview.listingId);
                            await redisClient.del(listingCacheKey);
                            
                            // Refresh listing cache with new review data
                            const currentListing = await prisma.listing.findUnique({
                                where: { id: currentReview.listingId },
                                include: {
                                    selectedMainCategories: true,
                                    selectedSubCategories: true,
                                    selectedSpecificItems: true,
                                    reviews: {
                                        where: { status: 'ACCEPTED' },
                                        select: { rating: true, comment: true, createdAt: true, user: { select: { fname: true, lname: true } } }
                                    },
                                    bookings: {
                                        select: { 
                                            id: true, status: true, createdAt: true, 
                                            user: { select: { fname: true, lname: true } }, 
                                            bookingDate: true, booking_hours: true, additionalNote: true, 
                                            ageGroup: true, numberOfPersons: true, paymentMethod: true 
                                        },
                                    }
                                }
                            });

                            if (currentListing && deeplClient) {
                                const acceptedReviews = currentListing.reviews;
                                const totalReviews = acceptedReviews.length;
                                const averageRating = totalReviews > 0
                                    ? acceptedReviews.reduce((sum, review) => sum + review.rating, 0) / totalReviews
                                    : 0;

                                const ratingDistribution = {
                                    5: acceptedReviews.filter(r => r.rating === 5).length,
                                    4: acceptedReviews.filter(r => r.rating === 4).length,
                                    3: acceptedReviews.filter(r => r.rating === 3).length,
                                    2: acceptedReviews.filter(r => r.rating === 2).length,
                                    1: acceptedReviews.filter(r => r.rating === 1).length
                                };

                                const listingWithStats = {
                                    ...currentListing,
                                    averageRating: Math.round(averageRating * 10) / 10,
                                    totalReviews,
                                    ratingDistribution,
                                    totalBookings: currentListing.bookings.length,
                                    confirmedBookings: currentListing.bookings.filter(b => b.status === 'CONFIRMED').length
                                };
                                
                                const translatedListing = await translateListingFields(listingWithStats, "AR", "EN");
                                await redisClient.setEx(listingCacheKey, AR_CACHE_EXPIRATION, JSON.stringify(translatedListing));
                            }
                        }
                    }

                    recordAuditLog(AuditLogAction.GENERAL_UPDATE, {
                        userId: reqDetails.actorUserId || currentReview.user.id,
                        entityName: 'Review',
                        entityId: reviewId.toString(),
                        oldValues: currentReview,
                        newValues: updatedReview,
                        description: `Review ${reviewId} updated.`,
                        ipAddress: reqDetails.ipAddress,
                        userAgent: reqDetails.userAgent,
                    });

                } catch (bgError) {
                    console.error(`Background task error for review update ${reviewId}:`, bgError);
                }
            });

            if (lang === 'ar' && deeplClient) {
                const translatedReview = { ...updatedReview };
                
                if (updatedReview.comment) {
                    translatedReview.comment = await translateText(updatedReview.comment, 'AR', 'EN');
                }
                if (updatedReview.status) {
                    translatedReview.status = await translateText(updatedReview.status, 'AR', 'EN');
                }
                if (updatedReview.listing && updatedReview.listing.name) {
                    translatedReview.listing = {
                        ...updatedReview.listing,
                        name: await translateText(updatedReview.listing.name, 'AR', 'EN')
                    };
                }
                
                return translatedReview;
            }
            
            return updatedReview;
        } catch (error) {
            console.error(`Failed to update review ${id}: ${error.message}`);
            throw new Error(`Failed to update review ${id}: ${error.message}`);
        }
    },

    // 6. Delete Review
    async deleteReview(id, reqDetails = {}) {
        try {
            const reviewId = parseInt(id);
            const reviewToDelete = await prisma.review.findUnique({ 
                where: { id: reviewId }, 
                include: { 
                    user: true, 
                    listing: { select: { name: true, id: true } },
                    booking: true
                } 
            });
            
            if (!reviewToDelete) throw new Error('Review not found');

            // Remove review reference from booking if it exists
            if (reviewToDelete.booking) {
                await prisma.booking.update({
                    where: { id: reviewToDelete.booking.id },
                    data: { review_id: null }
                });
            }

            const deletedReview = await prisma.review.delete({ where: { id: reviewId } });

            setImmediate(async () => {
                try {
                    // Clear relevant caches
                    if (redisClient.isReady) {
                        const keysToDel = [
                            cacheKeys.reviewAr(reviewId),
                            cacheKeys.userReviewsAr(reviewToDelete.user.uid),
                            cacheKeys.listingReviewsAr(reviewToDelete.listingId),
                            cacheKeys.listingAr(reviewToDelete.listingId)
                        ];
                        const allReviewsKeys = await redisClient.keys(cacheKeys.allReviewsAr('*'));
                        if (allReviewsKeys.length) keysToDel.push(...allReviewsKeys);
                        if (keysToDel.length > 0) await redisClient.del(keysToDel);

                        // Update listing cache after review deletion
                        if (reviewToDelete.listingId && deeplClient) {
                            const listingCacheKey = cacheKeys.listingAr(reviewToDelete.listingId);
                            await redisClient.del(listingCacheKey);
                            
                            // Refresh listing cache
                            const currentListing = await prisma.listing.findUnique({
                                where: { id: reviewToDelete.listingId },
                                include: {
                                    selectedMainCategories: true,
                                    selectedSubCategories: true,
                                    selectedSpecificItems: true,
                                    reviews: {
                                        where: { status: 'ACCEPTED' },
                                        select: { rating: true, comment: true, createdAt: true, user: { select: { fname: true, lname: true } } }
                                    },
                                    bookings: {
                                        select: { 
                                            id: true, status: true, createdAt: true, 
                                            user: { select: { fname: true, lname: true } }, 
                                            bookingDate: true, booking_hours: true, additionalNote: true, 
                                            ageGroup: true, numberOfPersons: true, paymentMethod: true 
                                        },
                                    }
                                }
                            });

                            if (currentListing) {
                                const acceptedReviews = currentListing.reviews;
                                const totalReviews = acceptedReviews.length;
                                const averageRating = totalReviews > 0
                                    ? acceptedReviews.reduce((sum, review) => sum + review.rating, 0) / totalReviews
                                    : 0;

                                const ratingDistribution = {
                                    5: acceptedReviews.filter(r => r.rating === 5).length,
                                    4: acceptedReviews.filter(r => r.rating === 4).length,
                                    3: acceptedReviews.filter(r => r.rating === 3).length,
                                    2: acceptedReviews.filter(r => r.rating === 2).length,
                                    1: acceptedReviews.filter(r => r.rating === 1).length
                                };

                                const listingWithStats = {
                                    ...currentListing,
                                    averageRating: Math.round(averageRating * 10) / 10,
                                    totalReviews,
                                    ratingDistribution,
                                    totalBookings: currentListing.bookings.length,
                                    confirmedBookings: currentListing.bookings.filter(b => b.status === 'CONFIRMED').length
                                };
                                
                                const translatedListing = await translateListingFields(listingWithStats, "AR", "EN");
                                await redisClient.setEx(listingCacheKey, AR_CACHE_EXPIRATION, JSON.stringify(translatedListing));
                            }
                        }
                    }

                    recordAuditLog(AuditLogAction.GENERAL_DELETE, {
                        userId: reqDetails.actorUserId,
                        entityName: 'Review',
                        entityId: reviewId.toString(),
                        oldValues: reviewToDelete,
                        description: `Review ${reviewId} for listing '${reviewToDelete.listing.name}' deleted.`,
                        ipAddress: reqDetails.ipAddress,
                        userAgent: reqDetails.userAgent,
                    });

                } catch (bgError) {
                    console.error(`Background task error for review deletion ${reviewId}:`, bgError);
                }
            });

            return { 
                message: `Review ${reviewId} deleted successfully.`, 
                deletedReviewId: deletedReview.id 
            };
        } catch (error) {
            console.error(`Failed to delete review ${id}: ${error.message}`);
            throw new Error(`Failed to delete review ${id}: ${error.message}`);
        }
    }
};

export default reviewService;
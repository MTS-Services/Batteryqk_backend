import prisma from '../utils/prismaClient.js';
import { recordAuditLog } from '../utils/auditLogHandler.js';
import { AuditLogAction } from '@prisma/client';
import { createClient } from "redis";
import * as deepl from "deepl-node";
import { sendMail } from '../utils/mailer.js';
import pLimit from 'p-limit';


// --- DeepL Configuration ---
const DEEPL_AUTH_KEY = process.env.DEEPL_AUTH_KEY;
const DEEPL_AUTH_KEY_2 = process.env.DEEPL_AUTH_KEY_2;

let currentKeyIndex = 0;
const deeplKeys = [DEEPL_AUTH_KEY, DEEPL_AUTH_KEY_2];

function getActiveDeepLClient() {
    return new deepl.Translator(deeplKeys[currentKeyIndex]);
}

function switchToNextKey() {
    currentKeyIndex = (currentKeyIndex + 1) % deeplKeys.length;
    console.log(`Switched to DeepL key ${currentKeyIndex + 1}`);
}

const deeplClient = getActiveDeepLClient();

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
const translationCache = new Map();
const limit = pLimit(5);

function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function translateText(text, targetLang, sourceLang = null) {
    let currentClient = getActiveDeepLClient();
    
    if (!currentClient) {
        console.warn("DeepL client is not initialized.");
        return text;
    }

    if (!text || typeof text !== 'string') {
        return text;
    }

    const cacheKey = `${text}::${sourceLang || 'auto'}::${targetLang}`;
    if (translationCache.has(cacheKey)) {
        return translationCache.get(cacheKey);
    }

    try {
        const result = await limit(async () => {
            let retries = 3;
            let keysSwitched = 0;
            
            while (retries > 0 && keysSwitched < deeplKeys.length) {
                try {
                    const res = await currentClient.translateText(text, sourceLang, targetLang);
                    return res;
                } catch (err) {
                    if (err.message.includes("Too many requests") || err.message.includes("quota")) {
                        console.warn(`DeepL rate limit/quota hit with key ${currentKeyIndex + 1}, switching key...`);
                        switchToNextKey();
                        currentClient = getActiveDeepLClient();
                        keysSwitched++;
                        await delay(500);
                        retries--;
                    } else {
                        throw err;
                    }
                }
            }
            throw new Error("Failed after trying all available DeepL keys.");
        });

        console.log(`Translated: "${text}" => "${result.text}"`);
        translationCache.set(cacheKey, result.text);
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
async function translateBookingFields(booking, targetLang, sourceLang = null) {
    console.log(`Translating booking fields to ${booking}...`);
    if (!booking) return booking;
    const translatedBooking = { ...booking };
    if (booking.additionalNote) {
        translatedBooking.additionalNote = await translateText(booking.additionalNote, targetLang, sourceLang);
    }
    if (booking.ageGroup) {
        translatedBooking.ageGroup = await translateText(booking.ageGroup, targetLang, sourceLang);
    }
    if (booking.status) {
        translatedBooking.status = await translateText(booking.status, targetLang, sourceLang);
    }
    if (booking.booking_hours) {
        translatedBooking.booking_hours = await translateText(booking.booking_hours, targetLang, sourceLang
        );
    }
    if (booking.paymentMethod) {
        translatedBooking.paymentMethod = await translateText(booking.paymentMethod, targetLang, sourceLang
        );
    }
    if (booking.user) {
        console.log(`Translating user fields for booking ${booking.id}...`);
        // DO NOT translate user's proper names. Preserve them.
        console.log(`Translating user name for booking ${booking.user.name}...`);
        console.log(`Translating user fname ${booking.user.fname} and lname ${booking.user.lname}...`);
        translatedBooking.user = {
            ...booking.user,
            fname: await translateText(booking.user.fname, targetLang, sourceLang),
            lname: await translateText(booking.user.lname, targetLang, sourceLang),
           
        };
    }
    if (booking.listing) {
        translatedBooking.listing = {
            ...booking.listing,
            name: await translateText(booking.listing.name, targetLang, sourceLang),
            description: await translateText(booking.listing.description, targetLang, sourceLang),
            facilities: booking.listing.facilities ? await Promise.all(booking.listing.facilities.map(f => translateText(f, targetLang, sourceLang))) : [],
            location: booking.listing.location ? await Promise.all(booking.listing.location.map(l => translateText(l, targetLang, sourceLang))) : [],
            agegroup: booking.listing.agegroup ? await Promise.all(booking.listing.agegroup.map(a => translateText(a, targetLang, sourceLang))) : [],
            operatingHours: booking.listing.operatingHours ? await Promise.all(booking.listing.operatingHours.map(o => translateText(o, targetLang, sourceLang))) : [],

        };
    }
    if (booking.review) {
        translatedBooking.review = {
            ...booking.review,
            status: await translateText(booking.review.status, targetLang, sourceLang),
            comment: await translateText(booking.review.comment, targetLang, sourceLang)
        };
    }

    if (booking.reward) {
        translatedBooking.reward = {
            ...booking.reward,
            description: await translateText(booking.reward.description, targetLang, sourceLang),
            category: await translateText(booking.reward.category, targetLang, sourceLang)
        };
    }
    return translatedBooking;
}
async function translateReviewFields(review, targetLang, sourceLang = null) {
    if (!review) return review;
    
    const translatedReview = { ...review };
    
    // Translate review fields
    if (review.comment) {
        translatedReview.comment = await translateText(review.comment, targetLang, sourceLang);
    }
    if (review.status) {
        translatedReview.status = await translateText(review.status, targetLang, sourceLang);
    }
    
    // Translate user fields if present
    if (review.user) {
        translatedReview.user = {
            ...review.user,
            fname: await translateText(review.user.fname, targetLang, sourceLang),
            lname: await translateText(review.user.lname, targetLang, sourceLang)
        };
    }
    
    // Translate listing fields if present
    if (review.listing) {
        translatedReview.listing = {
            ...review.listing,
            name: await translateText(review.listing.name, targetLang, sourceLang),
            description: review.listing.description ? await translateText(review.listing.description, targetLang, sourceLang) : null,
            agegroup: review.listing.agegroup ? await translateArrayFields(review.listing.agegroup, targetLang, sourceLang) : [],
            location: review.listing.location ? await translateArrayFields(review.listing.location, targetLang, sourceLang) : [],
            facilities: review.listing.facilities ? await translateArrayFields(review.listing.facilities, targetLang, sourceLang) : [],
            operatingHours: review.listing.operatingHours ? await translateArrayFields(review.listing.operatingHours, targetLang, sourceLang) : [],
        };
    }
    
    // Translate booking fields if present
    if (review.booking) {
        translatedReview.booking = {
            ...review.booking,
            additionalNote: review.booking.additionalNote ? await translateText(review.booking.additionalNote, targetLang, sourceLang) : null,
            ageGroup: review.booking.ageGroup ? await translateText(review.booking.ageGroup, targetLang, sourceLang) : null,
            status: review.booking.status ? await translateText(review.booking.status, targetLang, sourceLang) : null,
            booking_hours: review.booking.booking_hours ? await translateText(review.booking.booking_hours, targetLang, sourceLang) : null,
            paymentMethod: review.booking.paymentMethod ? await translateText(review.booking.paymentMethod, targetLang, sourceLang) : null
        };
    }
    
    return translatedReview;
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
                user: await translateText(review.user.fname, targetLang, sourceLang) + ' ' + await translateText(review.user.lname, targetLang, sourceLang),
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
                paymentMethod: booking.paymentMethod ? await translateText(booking.paymentMethod, targetLang, sourceLang) : null
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
            const { bookingId, rating, comment } = data;

            const user = await prisma.user.findUnique({ where: { uid: userUid } });
            if (!user) throw new Error('User not found');

            const booked = await prisma.booking.findUnique({
                where: { id: bookingId },
            });
            if (!booked) throw new Error('Booking not found');
            
            const listing = await prisma.listing.findUnique({
                where: { id: booked.listingId }
            });
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

                if (booking.status === 'PENDING') {
                    throw new Error('Cannot review a pending booking. Please wait for booking confirmation.');
                }

                if (booking.status === 'CANCELLED') {
                    throw new Error('Cannot review a cancelled booking.');
                }

                if (booking.paymentMethod !== 'PAID') {
                    throw new Error('You can only review bookings that have been paid for.');
                }

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
                    listingId: booked.listingId,
                    rating: parseInt(rating),
                    comment: dataForDb.comment,
                    status: 'PENDING',
                },
                include: { 
                    user: { select: { fname: true, lname: true, uid: true } }, 
                    listing: { select: { name: true, id: true, description: true, agegroup: true, location: true, facilities: true, operatingHours: true } },
                    booking: bookingId ? { select: { id: true, bookingDate: true, additionalNote: true, ageGroup: true, status: true, booking_hours: true, paymentMethod: true } } : false
                }
            });

            // Update booking with review reference if bookingId provided
            if (bookingId) {
                await prisma.booking.update({
                    where: { id: bookingId },
                    data: { review_id: review.id }
                });
            }

            // Cache the newly created review immediately in Arabic if applicable
            if (lang === 'ar' && deeplClient && redisClient.isReady) {
                try {
                    const translatedReview = await translateReviewFields(review, 'AR', 'EN');
                    await redisClient.setEx(cacheKeys.reviewAr(review.id), AR_CACHE_EXPIRATION, JSON.stringify(translatedReview));
                    console.log(`✅ Cached review ${review.id} in Arabic individually`);
                } catch (cacheError) {
                    console.error(`Failed to cache review ${review.id} individually:`, cacheError);
                }
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
                            cacheKeys.listingReviewsAr(booked.listingId),
                            cacheKeys.listingAr(booked.listingId),
                            cacheKeys.userBookingsAr(user.uid)
                        ];
                        const allReviewsKeys = await redisClient.keys(cacheKeys.allReviewsAr('*'));
                        if (allReviewsKeys.length) keysToDel.push(...allReviewsKeys);
                        if (keysToDel.length > 0) await redisClient.del(keysToDel);

                        // Re-fetch and cache the complete review with all relationships for Arabic
                        if (deeplClient) {
                            const reviewWithIncludes = await prisma.review.findUnique({
                                where: { id: review.id },
                                include: { 
                                    user: { select: { uid: true, fname: true, lname: true } }, 
                                    listing: { select: { name: true, id: true, description: true, agegroup: true, location: true, facilities: true, operatingHours: true } },
                                    booking: { select: { id: true, bookingDate: true, additionalNote: true, ageGroup: true, status: true, booking_hours: true, paymentMethod: true } }
                                }
                            });

                            if (reviewWithIncludes) {
                                const translatedReview = await translateReviewFields(reviewWithIncludes, 'AR', 'EN');
                                await redisClient.setEx(cacheKeys.reviewAr(review.id), AR_CACHE_EXPIRATION, JSON.stringify(translatedReview));
                                console.log(`✅ Re-cached complete review ${review.id} in Arabic individually with all relationships`);
                            }
                        }
                        
                        // Update booking cache if exists
                        if (bookingId && deeplClient) {
                            const updatedBooking = await prisma.booking.findUnique({
                                where: { id: bookingId },
                                include: { 
                                    user: { select: { id: true, fname: true, lname: true, email: true } }, 
                                    listing: true, 
                                    review: { select: { id: true, rating: true, comment: true, createdAt: true } }, 
                                    reward: true 
                                }
                            });
                            
                            if (updatedBooking) {
                                const translatedBooking = await translateBookingFields(updatedBooking, 'AR', 'EN');
                                await redisClient.setEx(cacheKeys.bookingAr(bookingId), AR_CACHE_EXPIRATION, JSON.stringify(translatedBooking));
                            }
                        }

                        // Update listing cache to reflect new review
                        const currentListing = await prisma.listing.findUnique({
                            where: { id: booked.listingId },
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
                            await redisClient.setEx(cacheKeys.listingAr(booked.listingId), AR_CACHE_EXPIRATION, JSON.stringify(translatedListing));
                        }
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

            // Build where clause from filters
            const whereClause = {
                ...(restFilters.status && { status: restFilters.status }),
                ...(restFilters.listingId && { listingId: parseInt(restFilters.listingId) }),
                ...(restFilters.rating && { rating: parseInt(restFilters.rating) }),
            };

            // Get review IDs from database first
            const [reviewIds, total] = await prisma.$transaction([
                prisma.review.findMany({
                    where: whereClause,
                    select: { id: true },
                    orderBy: { createdAt: 'desc' },
                    skip,
                    take: limitNum
                }),
                prisma.review.count({ where: whereClause })
            ]);

            console.log('Database review IDs found:', reviewIds.length);

            if (reviewIds.length === 0) {
                return {
                    reviews: [],
                    pagination: { total: 0, page: pageNum, limit: limitNum, totalPages: 0 },
                    message: lang === 'ar' ? 'لا توجد تقييمات متاحة' : 'No reviews available'
                };
            }

            const reviews = [];
            const missingReviewIds = [];

            // Try to get each review from individual cache entries
            if (lang === 'ar' && redisClient.isReady) {
                for (const { id } of reviewIds) {
                    const cacheKey = cacheKeys.reviewAr(id);
                    const cachedReview = await redisClient.get(cacheKey);
                    
                    if (cachedReview) {
                        reviews.push(JSON.parse(cachedReview));
                    } else {
                        missingReviewIds.push(id);
                    }
                }
                console.log(`Found ${reviews.length} cached reviews, ${missingReviewIds.length} missing from cache`);
            } else {
                missingReviewIds.push(...reviewIds.map(r => r.id));
            }

            // Fetch missing reviews from database
            if (missingReviewIds.length > 0) {
                const missingReviews = await prisma.review.findMany({
                    where: { id: { in: missingReviewIds } },
                    include: { 
                        user: { select: { uid: true, fname: true, lname: true } }, 
                        listing: { select: { name: true, id: true, description: true, agegroup: true, location: true, facilities: true, operatingHours: true } },
                        booking: { select: { id: true, bookingDate: true, additionalNote: true, ageGroup: true, status: true, booking_hours: true, paymentMethod: true } }
                    },
                    orderBy: { createdAt: 'desc' }
                });

                // Process missing reviews (translate if needed and cache individually)
                for (const review of missingReviews) {
                    let processedReview = review;

                    if (lang === 'ar' && deeplClient) {
                        const translatedReview = await translateReviewFields(review, 'AR', 'EN');
                        processedReview = translatedReview;

                        // Cache the translated review individually
                        if (redisClient.isReady) {
                            const cacheKey = cacheKeys.reviewAr(review.id);
                            await redisClient.setEx(cacheKey, AR_CACHE_EXPIRATION, JSON.stringify(translatedReview));
                        }
                    }

                    reviews.push(processedReview);
                }
            }

            // Sort reviews to maintain original order (by creation date desc)
            const sortedReviews = reviews.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

            return {
                reviews: sortedReviews,
                pagination: { total, page: pageNum, limit: limitNum, totalPages: Math.ceil(total / limitNum) }
            };
        } catch (error) {
            console.error(`Failed to get all reviews: ${error.message}`);
            throw new Error(`Failed to get all reviews: ${error.message}`);
        }
    },

    // 3. Get Review by ID
    async getReviewById(id, lang = 'en') {
        try {
            const reviewId = parseInt(id);
            //check if reviewId is a valid number
         const isValidId =  prisma.review.findUnique({
                where: { id: reviewId },
            });
            if (!isValidId) 
            {
              throw new Error('Invalid review ID');
            }

            const cacheKey = cacheKeys.reviewAr(reviewId);

            if (lang === 'ar' && redisClient.isReady) {
                const cachedReview = await redisClient.get(cacheKey);
                if (cachedReview) return JSON.parse(cachedReview);
            }

            const review = await prisma.review.findUnique({
                where: { id: reviewId },
                include: { 
                    user: { select: { uid: true, fname: true, lname: true } }, 
                    listing: { select: { name: true, id: true, description: true, agegroup: true, location: true, facilities: true, operatingHours: true } },
                    booking: { select: { id: true, bookingDate: true, additionalNote: true, ageGroup: true, status: true, booking_hours: true, paymentMethod: true } }
                }
            });

            if (!review) return null;

            if (lang === 'ar' && deeplClient) {
                const translatedReview = await translateReviewFields(review, 'AR', 'EN');
                
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
            console.log(`Fetching reviews for user UID: ${uid} in language: ${lang}`);
            //  find user by UID
            const user = await prisma.user.findUnique({ where: { uid: uid } });
            if (!user) throw new Error('User not found');
            const cacheKey = cacheKeys.userReviewsAr(uid);

            if (lang === 'ar' && redisClient.isReady) {
                const cachedReviews = await redisClient.get(cacheKey);
                if (cachedReviews) return JSON.parse(cachedReviews);
            }

            const reviews = await prisma.review.findMany({
                where: { user: { uid: uid } },
                include: { 
                    listing: { select: { name: true, id: true, description: true, agegroup: true, location: true, facilities: true, operatingHours: true } },
                    booking: { select: { id: true, bookingDate: true, additionalNote: true, ageGroup: true, status: true, booking_hours: true, paymentMethod: true } }
                },
                orderBy: { createdAt: 'desc' }
            });

            if (lang === 'ar' && deeplClient) {
                const translatedReviews = await Promise.all(reviews.map(review => translateReviewFields(review, 'AR', 'EN')));
                
                if (redisClient.isReady) await redisClient.setEx(cacheKey, AR_CACHE_EXPIRATION, JSON.stringify(translatedReviews));
                return translatedReviews;
            }

            return reviews;
        } catch (error) {
            console.error(`Failed to get reviews for user ${uid}: ${error.message}`);
            throw new Error(`Failed to get reviews for user ${uid}: ${error.message}`);
        }
    },

    // 5. Update Review
    async updateReview(id, data, userUid, lang = 'en', reqDetails = {}) {
        try {
            const reviewId = parseInt(id);
            console.log(lang);
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
            console.log(data);
            
            // Status translation mapping for Arabic to English
            const statusTranslationMap = {
                'مقبول': 'ACCEPTED',
                'مرفوض': 'REJECTED', 
                'قيد المراجعة': 'PENDING',
                'معلق': 'PENDING'
            };
            
            // Only allow editing rating and comment for regular users
            let updateData = {};
            if (data.rating !== undefined) updateData.rating = parseInt(data.rating);
            if (data.comment !== undefined) {
                if (lang === 'ar' && deeplClient) {
                    updateData.comment = await translateText(data.comment, 'EN-US', 'AR');
                    updateData.status = 'PENDING'; // Reset status to pending on comment update
                } else {
                    updateData.comment = data.comment;
                }
            }

            // Admin can update status
            if (data.status !== undefined && !userUid) {
                if(lang === 'ar' && data.status in statusTranslationMap) {
                    updateData.status = statusTranslationMap[data.status];
                } else {
                    updateData.status = data.status.toUpperCase();
                }
                console.log(updateData.status);
            }

            const updatedReview = await prisma.review.update({
                where: { id: reviewId },
                data: updateData,
                include: { 
                    user: { select: { uid: true, fname: true, lname: true } }, 
                    listing: { select: { name: true, id: true, description: true, agegroup: true, location: true, facilities: true, operatingHours: true } },
                    booking: { select: { id: true, bookingDate: true, additionalNote: true, ageGroup: true, status: true, booking_hours: true, paymentMethod: true } }
                }
            });

            // Return immediately with translated response if needed
            const immediateResponse = lang === 'ar' && deeplClient ? 
                await translateReviewFields(updatedReview, 'AR', 'EN') : 
                updatedReview;

            setImmediate(async () => {
                try {
                    // Send notification for status updates (admin action)
                    if (data.status && data.status !== currentReview.status) {
                        const statusMessage = updateData.status === 'ACCEPTED' ? 
                            `Your review for ${currentReview.listing.name} has been approved and is now live.` :
                            `Your review for ${currentReview.listing.name} has been ${updateData.status.toLowerCase()}.`;
                        
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
                            status: updateData.status
                        });
                    }

                    // Clear relevant caches
                    if (redisClient.isReady) {
                        const keysToDel = [
                            cacheKeys.reviewAr(reviewId),
                            cacheKeys.userReviewsAr(currentReview.user.uid),
                            cacheKeys.listingReviewsAr(currentReview.listingId),
                            cacheKeys.listingAr(currentReview.listingId),
                            cacheKeys.userBookingsAr(currentReview.user.uid)
                        ];
                        
                        // Clear all reviews cache
                        const allReviewsKeys = await redisClient.keys(cacheKeys.allReviewsAr('*'));
                        if (allReviewsKeys.length) keysToDel.push(...allReviewsKeys);
                        
                        if (keysToDel.length > 0) await redisClient.del(keysToDel);

                        // Update individual review cache
                        if (deeplClient) {
                            const translatedReview = await translateReviewFields(updatedReview, 'AR', 'EN');
                            await redisClient.setEx(cacheKeys.reviewAr(reviewId), AR_CACHE_EXPIRATION, JSON.stringify(translatedReview));
                        }

                        // Update booking cache if review is linked to a booking
                        if (updatedReview.booking) {
                            const bookingWithReview = await prisma.booking.findUnique({
                                where: { id: updatedReview.booking.id },
                                include: { 
                                    user: { select: { id: true, fname: true, lname: true, email: true } }, 
                                    listing: true, 
                                    review: { select: { id: true, rating: true, comment: true, createdAt: true } }, 
                                    reward: true 
                                }
                            });
                            
                            if (bookingWithReview && deeplClient) {
                                const translatedBooking = await translateBookingFields(bookingWithReview, 'AR', 'EN');
                                await redisClient.setEx(cacheKeys.bookingAr(updatedReview.booking.id), AR_CACHE_EXPIRATION, JSON.stringify(translatedBooking));
                            }
                        }

                        // Update listing cache to reflect review changes
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
                            await redisClient.setEx(cacheKeys.listingAr(currentReview.listingId), AR_CACHE_EXPIRATION, JSON.stringify(translatedListing));
                        }

                        // Update user reviews cache
                        const userReviews = await prisma.review.findMany({
                            where: { user: { uid: currentReview.user.uid } },
                            include: { 
                                listing: { select: { name: true, id: true, description: true, agegroup: true, location: true, facilities: true, operatingHours: true } },
                                booking: { select: { id: true, bookingDate: true, additionalNote: true, ageGroup: true, status: true, booking_hours: true, paymentMethod: true } }
                            },
                            orderBy: { createdAt: 'desc' }
                        });

                        if (userReviews.length > 0 && deeplClient) {
                            const translatedUserReviews = await Promise.all(
                                userReviews.map(r => translateReviewFields(r, 'AR', 'EN'))
                            );
                            await redisClient.setEx(cacheKeys.userReviewsAr(currentReview.user.uid), AR_CACHE_EXPIRATION, JSON.stringify(translatedUserReviews));
                        }

                        // Update user bookings cache if review affects booking
                        if (updatedReview.booking) {
                            const userBookings = await prisma.booking.findMany({
                                where: { user: { uid: currentReview.user.uid } },
                                include: { listing: true, review: true, reward: true },
                                orderBy: { createdAt: 'desc' }
                            });

                            if (userBookings.length > 0 && deeplClient) {
                                const translatedUserBookings = await Promise.all(
                                    userBookings.map(b => translateBookingFields(b, 'AR', 'EN'))
                                );
                                await redisClient.setEx(cacheKeys.userBookingsAr(currentReview.user.uid), AR_CACHE_EXPIRATION, JSON.stringify(translatedUserBookings));
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

            return immediateResponse;
        } catch (error) {
            console.error(`Failed to update review ${id}: ${error.message}`);
            throw new Error(`Failed to update review ${id}: ${error.message}`);
        }
    },

    // 6. Delete Review
    async deleteReview(id, lang = 'en', reqDetails = {}) {
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

            // Return immediately with translated response if needed
            const immediateResponse = lang === 'ar' ? {
                message: 'تم حذف التقييم بنجاح.',
                deletedReviewId: deletedReview.id
            } : {
                message: `Review ${reviewId} deleted successfully.`,
                deletedReviewId: deletedReview.id
            };

            setImmediate(async () => {
                try {
                    // Send notification to user about review deletion
                    const notificationMessage = lang === 'ar' ? 
                        `تم حذف تقييمك لـ ${reviewToDelete.listing.name}.` :
                        `Your review for ${reviewToDelete.listing.name} has been deleted.`;
                    
                    await prisma.notification.create({
                        data: {
                            userId: reviewToDelete.user.id,
                            title: lang === 'ar' ? 'تم حذف التقييم' : 'Review Deleted',
                            message: notificationMessage,
                            type: 'GENERAL',
                            entityId: reviewId.toString(),
                            entityType: 'Review'
                        }
                    });

                    // Send email notification
                    const emailSubject = lang === 'ar' ? 'تم حذف التقييم' : 'Review Deleted';
                    const emailMessage = lang === 'ar' ? 
                        `مرحباً ${reviewToDelete.user.fname || 'العميل'},\n\nتم حذف تقييمك لـ: ${reviewToDelete.listing.name}.\n\nشكراً لك.` :
                        `Hello ${reviewToDelete.user.fname || 'Customer'},\n\nYour review for: ${reviewToDelete.listing.name} has been deleted.\n\nThank you.`;
                    
                    await sendMail(reviewToDelete.user.email, emailSubject, emailMessage, lang, {
                        name: reviewToDelete.user.fname || 'Customer',
                        listingName: reviewToDelete.listing.name
                    });

                    // Clear relevant caches
                    if (redisClient.isReady) {
                        const keysToDel = [
                            cacheKeys.reviewAr(reviewId),
                            cacheKeys.userReviewsAr(reviewToDelete.user.uid),
                            cacheKeys.listingReviewsAr(reviewToDelete.listingId),
                            cacheKeys.listingAr(reviewToDelete.listingId),
                            cacheKeys.userBookingsAr(reviewToDelete.user.uid)
                        ];
                        
                        if (reviewToDelete.booking) {
                            keysToDel.push(cacheKeys.bookingAr(reviewToDelete.booking.id));
                        }
                        
                        const allReviewsKeys = await redisClient.keys(cacheKeys.allReviewsAr('*'));
                        if (allReviewsKeys.length) keysToDel.push(...allReviewsKeys);
                        if (keysToDel.length > 0) await redisClient.del(keysToDel);

                        // Update booking cache if review was linked to a booking
                        if (reviewToDelete.booking && deeplClient) {
                            const updatedBooking = await prisma.booking.findUnique({
                                where: { id: reviewToDelete.booking.id },
                                include: { 
                                    user: { select: { id: true, fname: true, lname: true, email: true } }, 
                                    listing: true, 
                                    review: { select: { id: true, rating: true, comment: true, createdAt: true } }, 
                                    reward: true 
                                }
                            });
                            
                            if (updatedBooking) {
                                const translatedBooking = await translateBookingFields(updatedBooking, 'AR', 'EN');
                                await redisClient.setEx(cacheKeys.bookingAr(reviewToDelete.booking.id), AR_CACHE_EXPIRATION, JSON.stringify(translatedBooking));
                            }
                        }

                        // Update listing cache to reflect review deletion and recalculate stats
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
                            await redisClient.setEx(cacheKeys.listingAr(reviewToDelete.listingId), AR_CACHE_EXPIRATION, JSON.stringify(translatedListing));
                        }

                        // Update user reviews cache
                        const userReviews = await prisma.review.findMany({
                            where: { user: { uid: reviewToDelete.user.uid } },
                            include: { 
                                listing: { select: { name: true, id: true, description: true, agegroup: true, location: true, facilities: true, operatingHours: true } },
                                booking: { select: { id: true, bookingDate: true, additionalNote: true, ageGroup: true, status: true, booking_hours: true, paymentMethod: true } }
                            },
                            orderBy: { createdAt: 'desc' }
                        });

                        if (deeplClient) {
                            const translatedUserReviews = await Promise.all(
                                userReviews.map(r => translateReviewFields(r, 'AR', 'EN'))
                            );
                            await redisClient.setEx(cacheKeys.userReviewsAr(reviewToDelete.user.uid), AR_CACHE_EXPIRATION, JSON.stringify(translatedUserReviews));
                        }

                        // Update user bookings cache if review affected booking
                        if (reviewToDelete.booking) {
                            const userBookings = await prisma.booking.findMany({
                                where: { user: { uid: reviewToDelete.user.uid } },
                                include: { listing: true, review: true, reward: true },
                                orderBy: { createdAt: 'desc' }
                            });

                            if (userBookings.length > 0 && deeplClient) {
                                const translatedUserBookings = await Promise.all(
                                    userBookings.map(b => translateBookingFields(b, 'AR', 'EN'))
                                );
                                await redisClient.setEx(cacheKeys.userBookingsAr(reviewToDelete.user.uid), AR_CACHE_EXPIRATION, JSON.stringify(translatedUserBookings));
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

            return immediateResponse;
        } catch (error) {
            console.error(`Failed to delete review ${id}: ${error.message}`);
            throw new Error(`Failed to delete review ${id}: ${error.message}`);
        }
    }
};

export default reviewService;

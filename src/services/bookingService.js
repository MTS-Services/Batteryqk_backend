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

//no expiration for AR cache
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

redisClient.on('error', (err) => console.error('Redis: Booking Cache - Error ->', err.message));
(async () => {
    try {
        await redisClient.connect();
        console.log('Redis: Booking Cache - Connected successfully.');
    } catch (err) {
        console.error('Redis: Booking Cache - Could not connect ->', err.message);
    }
})();

const cacheKeys = {
    bookingAr: (bookingId) => `booking:${bookingId}:ar`,
    userBookingsAr: (uid) => `user:${uid}:bookings:ar`,
    userNotificationsAr: (uid) => `user:${uid}:notifications:ar`,
    allBookingsAr: (filterHash = '') => `bookings:all${filterHash}:ar`,
    listingBookingsAr: (listingId) => `listing:${listingId}:bookings:ar`,
    listingAr: (listingId) => `listing:${listingId}:ar`,
    reviewAr: (reviewId) => `review:${reviewId}:ar`,
    userReviewsAr: (uid) => `user:${uid}:reviews:ar`,
    
};

// --- Helper Functions ---
// async function translateText(text, targetLang, sourceLang = null) {
//     if (!deeplClient || !text || typeof text !== 'string') return text;
//     try {
//         const result = await deeplClient.translateText(text, sourceLang, targetLang);
//         return result.text;
//     } catch (error) {
//         console.error(`DeepL Translation error: ${error.message}`);
//         return text;
//     }
// }

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



async function translateReviewFields(review, targetLang, sourceLang = null) {
    if (!review) return review;
    
    const translatedReview = { ...review };
    
    // Translate review fields
    if (review.comment) {
        translatedReview.comment = await translateText(review.comment, targetLang, sourceLang);
    }
    // if (review.status) {
    //     translatedReview.status = await translateText(review.status, targetLang, sourceLang);
    // }
    
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
            //status: review.booking.status ? await translateText(review.booking.status, targetLang, sourceLang) : null,
            booking_hours: review.booking.booking_hours ? await translateText(review.booking.booking_hours, targetLang, sourceLang) : null,
            paymentMethod: review.booking.paymentMethod ? await translateText(review.booking.paymentMethod, targetLang, sourceLang) : null
        };
    }
    
    return translatedReview;
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
    // if (booking.status) {
    //     translatedBooking.status = await translateText(booking.status, targetLang, sourceLang);
    // }
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
            gender: booking.listing.gender ? await translateText(booking.listing.gender, targetLang, sourceLang) : null,
            discount: booking.listing.discount ? await translateText(booking.listing.discount, targetLang, sourceLang) : null

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

async function updateRewardCategory(userId, totalPoints) {
    let category = 'BRONZE';
    if (totalPoints >= 2500) category = 'PLATINUM';
    else if (totalPoints >= 2000) category = 'GOLD';
    else if (totalPoints >= 1000) category = 'SILVER';

    const currentReward = await prisma.reward.findFirst({ where: { userId }, orderBy: { createdAt: 'desc' } });
    if (!currentReward || currentReward.category !== category) {
        await prisma.reward.create({ data: { userId, points: 0, category, description: `Upgraded to ${category} tier.` } });
        await prisma.notification.create({ data: { userId, title: "Reward Tier Upgraded!", message: `Congratulations! You've been upgraded to the ${category} tier.`, type: 'LOYALTY', entityId: userId.toString(), entityType: 'Reward' } });
    }
}

async function sendBookingEmails(booking, listing, user, lang) {
    try {
        const userSubject = lang === 'ar' ? 'تأكيد استلام طلب الحجز' : 'Booking Request Received';
        const userMessage = lang === 'ar' ? `مرحباً ${user.fname || 'العميل'},\n\nلقد استلمنا طلب الحجز الخاص بك لـ: ${listing.name} وهو الآن قيد المراجعة.\n\nتاريخ الحجز المطلوب: ${booking.bookingDate}` : `Hello ${user.fname || 'Customer'},\n\nWe have received your booking request for: ${listing.name}. It is now pending confirmation.\n\nRequested Booking Date: ${booking.bookingDate}`;
        await sendMail(user.email, userSubject, userMessage, lang, { name: user.fname || 'Customer', listingName: listing.name });

        const adminMessage = `Hello Admin,\n\nA new booking requires your confirmation.\n\nListing: ${listing.name}\nCustomer: ${user.fname} ${user.lname} (${user.email})\nBooking Date: ${booking.bookingDate}\nBooking Hours: ${booking.booking_hours || 'N/A'}\nGuests: ${booking.numberOfPersons}`;
        await sendMail(process.env.EMAIL_USER, 'New Booking - Confirmation Required', adminMessage, 'en', { customerName: `${user.fname} ${user.lname}`, listingName: listing.name });
    } catch (error) {
        console.error('Email sending error:', error);
    }
}

function createFilterHash(filters) {
    const sortedFilters = Object.keys(filters).sort().reduce((result, key) => { result[key] = filters[key]; return result; }, {});
    return JSON.stringify(sortedFilters);
}



async function translateArrayFields(arr, targetLang, sourceLang = null) {
    if (!arr || !Array.isArray(arr)) return arr;
    return await Promise.all(arr.map(item => translateText(item, targetLang, sourceLang)));
}

async function translateListingFields(listing, targetLang, sourceLang = null) {
    if (!listing) return listing;

    const translatedListing = { ...listing };

    if (listing.agegroup)
        translatedListing.agegroup = await translateArrayFields(listing.agegroup, targetLang, sourceLang);
    if (listing.discount)
        translatedListing.discount = await translateText(listing.discount, targetLang, sourceLang);

    if (listing.gender)
        translatedListing.gender = await translateText(listing.gender, targetLang, sourceLang);

    if (listing.discount)
        translatedListing.discount = await translateText(listing.discount, targetLang, sourceLang);

    // Translate basic text fields
    if (listing.name)
        translatedListing.name = await translateText(listing.name, targetLang, sourceLang);

    if (listing.description)
        translatedListing.description = await translateText(listing.description, targetLang, sourceLang);

    // Translate array fields
    const arrayFields = ['agegroup', 'location', 'facilities', 'operatingHours'];
    for (const field of arrayFields) {
        if (Array.isArray(listing[field])) // Ensure it's an array
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
                // DO NOT translate user's proper names. Preserve them.
                user: await translateText(review.user.fname, targetLang, sourceLang) + ' ' + await translateText(review.user.lname, targetLang, sourceLang)
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
                //status: await translateText(booking.status, targetLang, sourceLang),
                booking_hours: booking.booking_hours
                    ? await translateText(booking.booking_hours, targetLang, sourceLang)
                    : null,
                // DO NOT translate user's proper names. Preserve them.
                user: await translateText(booking.user.fname, targetLang, sourceLang) + ' ' + await translateText(booking.user.lname, targetLang, sourceLang),
                paymentMethod: await translateText(booking.paymentMethod, targetLang, sourceLang)
            }))
        );
    }
    
    return translatedListing;
}

// --- Booking Service ---
const bookingService = {

    // 1. Create Booking
    async createBooking(data, userUid, lang = 'en', reqDetails = {}) {
        try {
            const { listingId, bookingDate, booking_hours, additionalNote, numberOfPersons, ageGroup } = data;

            const user = await prisma.user.findUnique({ where: { uid: userUid } });
            if (!user) throw new Error('User not found');

            const listing = await prisma.listing.findUnique({ where: { id: listingId } });
            if (!listing) throw new Error('Listing not found');

            let dataForDb = { additionalNote, booking_hours, ageGroup };
            if (lang === 'ar' && deeplClient) {
                dataForDb.additionalNote = await translateText(additionalNote, 'EN-US', 'AR');
                dataForDb.booking_hours = await translateText(booking_hours, 'EN-US', 'AR');
                dataForDb.ageGroup = await translateText(ageGroup, 'EN-US', 'AR');
            }

            const booking = await prisma.booking.create({
                data: {
                    userId: user.id,
                    listingId: listingId,
                    bookingDate: bookingDate ? new Date(bookingDate) : null,
                    booking_hours: dataForDb.booking_hours,
                    additionalNote: dataForDb.additionalNote,
                    ageGroup: dataForDb.ageGroup,
                    numberOfPersons: numberOfPersons ? parseInt(numberOfPersons) : null,
                    status: 'PENDING',
                    paymentMethod: 'UNPAID',
                    updatedAt: new Date(),
                },
                include: { user: true, listing: true }
            });

            const immediateResponse = lang === 'ar' ? {
                message: 'تم استلام طلب الحجز بنجاح وسنعود إليك قريباً بالتأكيد.',
                bookingId: booking.id,
                listingName: await translateText(listing.name, 'AR', 'EN'),
                status: await translateText('PENDING', 'AR', 'EN'),
                paymentMethod: await translateText('UNPAID', 'AR', 'EN')
            } : {
                message: 'Booking request received successfully. We will get back to you with a confirmation shortly.',
                bookingId: booking.id,
                listingName: listing.name,
                status: 'PENDING',
                paymentMethod: 'UNPAID'
            };

            setImmediate(async () => {
                try {
                    const newReward = await prisma.reward.create({
                        data: { userId: user.id, bookingId: booking.id, points: 50, description: `Reward for booking request: ${listing.name}`, category: 'BRONZE' }
                    });
                    const totalPointsResult = await prisma.reward.aggregate({ where: { userId: user.id }, _sum: { points: true } });
                    await updateRewardCategory(user.id, totalPointsResult._sum.points || 0);
                    await prisma.notification.create({ data: { userId: user.id, title: 'Booking Request Received', message: `Your booking for ${listing.name} is pending confirmation.`, type: 'BOOKING', entityId: booking.id.toString(), entityType: 'Booking' } });
                    await prisma.notification.create({ data: { userId: user.id, title: 'Points Awarded!', message: `You've earned 50 points for your new booking request.`, type: 'LOYALTY', entityId: newReward.id.toString(), entityType: 'Reward' } });
                    await sendBookingEmails(booking, listing, user, lang);

                    if (redisClient.isReady && deeplClient) {
                        const cacheKeysToDel = [
                            cacheKeys.userBookingsAr(user.uid), 
                            cacheKeys.listingBookingsAr(listingId),
                            cacheKeys.listingAr(listingId)
                        ];
                        const allBookingsKeys = await redisClient.keys(cacheKeys.allBookingsAr('*'));
                        if (allBookingsKeys.length) cacheKeysToDel.push(...allBookingsKeys);
                        if (cacheKeysToDel.length > 0) await redisClient.del(cacheKeysToDel);

                        // Get booking with all includes for caching
                        const bookingWithIncludes = await prisma.booking.findUnique({
                            where: { id: booking.id },
                            include: { user: { select: { id: true, fname: true, lname: true, email: true } }, listing: true, review: { select: { id: true, rating: true, comment: true, createdAt: true } }, reward: true }
                        });
                        
                        if (bookingWithIncludes) {
                           
                            const translatedBooking = await translateBookingFields(bookingWithIncludes, 'AR', 'EN');
                            const bookingCacheKey = cacheKeys.bookingAr(booking.id);
                            await redisClient.setEx(bookingCacheKey, AR_CACHE_EXPIRATION, JSON.stringify(translatedBooking));
                        }

                        // Update user bookings cache in Arabic
                        const userBookings = await prisma.booking.findMany({
                            where: { user: { uid: user.uid } },
                            include: { listing: true, review: true, reward: true },
                            orderBy: { createdAt: 'desc' }
                        });
                        console.log(userBookings);

                        if (userBookings.length > 0) {
                            const translatedUserBookings = await Promise.all(
                                userBookings.map(b => translateBookingFields(b, 'AR', 'EN'))
                            );
                            await redisClient.setEx(cacheKeys.userBookingsAr(user.uid), AR_CACHE_EXPIRATION, JSON.stringify(translatedUserBookings));
                        }
                        
                        // Update listing cache in Arabic
                        const currentListing = await prisma.listing.findUnique({
                            where: { id: listingId },
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
                            await redisClient.setEx(cacheKeys.listingAr(listingId), AR_CACHE_EXPIRATION, JSON.stringify(translatedListing));
                        }
                    }
                } catch (bgError) {
                    console.error(`Background task error for booking ${booking.id}:`, bgError);
                }
            });

            recordAuditLog(AuditLogAction.BOOKING_CREATED, {
                userId: user.id,
                entityName: 'Booking',
                entityId: booking.id.toString(),
                newValues: booking,
                description: `Booking request for '${listing.name}' created by ${user.email}.`,
                ipAddress: reqDetails.ipAddress,
                userAgent: reqDetails.userAgent,
            });

            return immediateResponse;
        } catch (error) {
            console.error(`Failed to create booking: ${error.message}`);
            throw new Error(`Failed to create booking: ${error.message}`);
        }
    },

    // 2. Get All Bookings with Filters
    async getAllBookings(filters = {}, lang = 'en') {
        try {
            const { page = 1, limit = 10, ...restFilters } = filters;
            const pageNum = parseInt(page);
            const limitNum = parseInt(limit);
            const skip = (pageNum - 1) * limitNum;

            // Build where clause from filters
            const whereClause = {
                ...(restFilters.status && { status: restFilters.status }),
                ...(restFilters.listingId && { listingId: parseInt(restFilters.listingId) }),
            };

            // Get booking IDs from database first
            const [bookingIds, total] = await prisma.$transaction([
                prisma.booking.findMany({
                    where: whereClause,
                    select: { id: true },
                    orderBy: { createdAt: 'desc' },
                    skip,
                    take: limitNum
                }),
                prisma.booking.count({ where: whereClause })
            ]);

            console.log('Database booking IDs found:', bookingIds.length);

            // Check if no bookings available in database
            if (bookingIds.length === 0) {
                return {
                    bookings: [],
                    pagination: { total: 0, page: pageNum, limit: limitNum, totalPages: 0 },
                    message: lang === 'ar' ? 'لا توجد حجوزات متاحة' : 'No bookings available'
                };
            }

            const bookings = [];
            const missingBookingIds = [];

            // Try to get each booking from individual cache entries
            if (lang === 'ar' && redisClient.isReady) {
                for (const { id } of bookingIds) {
                    const cacheKey = cacheKeys.bookingAr(id);
                    const cachedBooking = await redisClient.get(cacheKey);
                    
                    if (cachedBooking) {
                        bookings.push(JSON.parse(cachedBooking));
                    } else {
                        missingBookingIds.push(id);
                    }
                }
                console.log(`Found ${bookings.length} cached bookings, ${missingBookingIds.length} missing from cache`);
            } else {
                // If not Arabic or Redis not ready, get all from DB
                missingBookingIds.push(...bookingIds.map(b => b.id));
            }

            // Fetch missing bookings from database
            if (missingBookingIds.length > 0) {
                const missingBookings = await prisma.booking.findMany({
                    where: { id: { in: missingBookingIds } },
                    include: { user: {select: {id:true, fname: true, lname: true,email:true}}, listing: true, review: true, reward: true },
                    orderBy: { createdAt: 'desc' }
                });

                // Process missing bookings (translate if needed and cache individually)
                for (const booking of missingBookings) {
                    let processedBooking = booking;

                    if (lang === 'ar' && deeplClient) {
                        const translatedBooking = { ...booking };
                        
                        // Translate booking fields
                        if (booking.additionalNote) {
                            translatedBooking.additionalNote = await translateText(booking.additionalNote, 'AR', 'EN');
                        }
                        if (booking.ageGroup) {
                            translatedBooking.ageGroup = await translateText(booking.ageGroup, 'AR', 'EN');
                        }
                        // if (booking.status) {
                        //     translatedBooking.status = await translateText(booking.status, 'AR', 'EN');
                        // }
                        if (booking.booking_hours) {
                            translatedBooking.booking_hours = await translateText(booking.booking_hours, 'AR', 'EN');
                        }
                        if (booking.paymentMethod) {
                            translatedBooking.paymentMethod = await translateText(booking.paymentMethod, 'AR', 'EN');
                        }
                        
                        // Translate listing fields if present
                        if (booking.listing) {
                            translatedBooking.listing = { ...booking.listing };
                            
                            if (booking.listing.name) {
                                translatedBooking.listing.name = await translateText(booking.listing.name, 'AR', 'EN');
                            }
                            if (booking.listing.description) {
                                translatedBooking.listing.description = await translateText(booking.listing.description, 'AR', 'EN');
                            }
                            if (Array.isArray(booking.listing.agegroup)) {
                                translatedBooking.listing.agegroup = await translateArrayFields(booking.listing.agegroup, 'AR', 'EN');
                            }
                            if (Array.isArray(booking.listing.location)) {
                                translatedBooking.listing.location = await translateArrayFields(booking.listing.location, 'AR', 'EN');
                            }
                            if (Array.isArray(booking.listing.facilities)) {
                                translatedBooking.listing.facilities = await translateArrayFields(booking.listing.facilities, 'AR', 'EN');
                            }
                            if (Array.isArray(booking.listing.operatingHours)) {
                                translatedBooking.listing.operatingHours = await translateArrayFields(booking.listing.operatingHours, 'AR', 'EN');
                            }
                        }
                        
                        // Translate review comment if present
                        if (booking.review && booking.review.comment) {
                            translatedBooking.review = {
                                ...booking.review,
                                comment: await translateText(booking.review.comment, 'AR', 'EN')
                            };
                        }
                        
                        // Translate reward fields if present
                        if (booking.reward) {
                            translatedBooking.reward = {
                                ...booking.reward,
                                description: await translateText(booking.reward.description, 'AR', 'EN'),
                                category: await translateText(booking.reward.category, 'AR', 'EN')
                            };
                        }
                        
                        processedBooking = translatedBooking;

                        // Cache the translated booking individually
                        if (redisClient.isReady) {
                            const cacheKey = cacheKeys.bookingAr(booking.id);
                            await redisClient.setEx(cacheKey, AR_CACHE_EXPIRATION, JSON.stringify(translatedBooking));
                        }
                    }

                    bookings.push(processedBooking);
                }
            }

            // Sort bookings to maintain original order (by creation date desc)
            const sortedBookings = bookings.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

            return {
                bookings: sortedBookings,
                pagination: { total, page: pageNum, limit: limitNum, totalPages: Math.ceil(total / limitNum) }
            };
        } catch (error) {
            console.error(`Failed to get all bookings: ${error.message}`);
            throw new Error(`Failed to get all bookings: ${error.message}`);
        }
    },

    // 3. Get Booking by ID
    async getBookingById(id, lang = 'en') {
        try {
            const bookingId = parseInt(id);
            const cacheKey = cacheKeys.bookingAr(bookingId);

            if (lang === 'ar' && redisClient.isReady) {
                const cachedBooking = await redisClient.get(cacheKey);
                if (cachedBooking) return JSON.parse(cachedBooking);
            }

            const booking = await prisma.booking.findUnique({
                where: { id: bookingId },
                include: { user: {
                    select: { id: true, fname: true, lname: true, email: true }
                }, listing: true, review: true, reward: true }
            });

            if (!booking) return null;

            if (lang === 'ar' && deeplClient) {
                const translatedBooking = { ...booking };
                
                // Translate booking fields
                if (booking.additionalNote) {
                    translatedBooking.additionalNote = await translateText(booking.additionalNote, 'AR', 'EN');
                }
                if (booking.ageGroup) {
                    translatedBooking.ageGroup = await translateText(booking.ageGroup, 'AR', 'EN');
                }
                // if (booking.status) {
                //     translatedBooking.status = await translateText(booking.status, 'AR', 'EN');
                // }
                if (booking.booking_hours) {
                    translatedBooking.booking_hours = await translateText(booking.booking_hours, 'AR', 'EN');
                }
                if (booking.paymentMethod) {
                    translatedBooking.paymentMethod = await translateText(booking.paymentMethod, 'AR', 'EN');
                }
                
                // Translate listing fields if present
                if (booking.listing) {
                    translatedBooking.listing = { ...booking.listing };
                    
                    if (booking.listing.name) {
                        translatedBooking.listing.name = await translateText(booking.listing.name, 'AR', 'EN');
                    }
                    if (booking.listing.description) {
                        translatedBooking.listing.description = await translateText(booking.listing.description, 'AR', 'EN');
                    }
                    if (Array.isArray(booking.listing.agegroup)) {
                        translatedBooking.listing.agegroup = await translateArrayFields(booking.listing.agegroup, 'AR', 'EN');
                    }
                    if (Array.isArray(booking.listing.location)) {
                        translatedBooking.listing.location = await translateArrayFields(booking.listing.location, 'AR', 'EN');
                    }
                    if (Array.isArray(booking.listing.facilities)) {
                        translatedBooking.listing.facilities = await translateArrayFields(booking.listing.facilities, 'AR', 'EN');
                    }
                    if (Array.isArray(booking.listing.operatingHours)) {
                        translatedBooking.listing.operatingHours = await translateArrayFields(booking.listing.operatingHours, 'AR', 'EN');
                    }
                }
                
                // Translate review comment if present
                if (booking.review && booking.review.comment) {
                    translatedBooking.review = {
                        ...booking.review,
                        comment: await translateText(booking.review.comment, 'AR', 'EN')
                    };
                }
                
                // Translate reward fields if present
                if (booking.reward) {
                    translatedBooking.reward = {
                        ...booking.reward,
                        description: await translateText(booking.reward.description, 'AR', 'EN'),
                        category: await translateText(booking.reward.category, 'AR', 'EN')
                    };
                }
                
                if (redisClient.isReady) await redisClient.setEx(cacheKey, AR_CACHE_EXPIRATION, JSON.stringify(translatedBooking));
                return translatedBooking;
            }

            return booking;
        } catch (error) {
            console.error(`Failed to get booking by ID ${id}: ${error.message}`);
            throw new Error(`Failed to get booking by ID ${id}: ${error.message}`);
        }
    },

    // 4. Get Bookings by User UID
    async getBookingsByUserUid(uid, lang = 'en') {
        try {
            const cacheKey = cacheKeys.userBookingsAr(uid);

            if (lang === 'ar' && redisClient.isReady) {
                const cachedBookings = await redisClient.get(cacheKey);
                if (cachedBookings) return JSON.parse(cachedBookings);
            }

            const bookings = await prisma.booking.findMany({
                where: { user: { uid: uid } },
                include: { listing: true, review: true, reward: true },
                orderBy: { createdAt: 'desc' }
            });

            if (lang === 'ar' && deeplClient) {
                const translatedBookings = await Promise.all(bookings.map(async (booking) => {
                    const translatedBooking = { ...booking };
                    
                    // Translate booking fields
                    if (booking.additionalNote) {
                        translatedBooking.additionalNote = await translateText(booking.additionalNote, 'AR', 'EN');
                    }
                    if (booking.ageGroup) {
                        translatedBooking.ageGroup = await translateText(booking.ageGroup, 'AR', 'EN');
                    }
                    // if (booking.status) {
                    //     translatedBooking.status = await translateText(booking.status, 'AR', 'EN');
                    // }
                    if (booking.booking_hours) {
                        translatedBooking.booking_hours = await translateText(booking.booking_hours, 'AR', 'EN');
                    }
                    if (booking.paymentMethod) {
                        translatedBooking.paymentMethod = await translateText(booking.paymentMethod, 'AR', 'EN');
                    }
                    
                    // Translate listing fields if present
                    if (booking.listing) {
                        translatedBooking.listing = { ...booking.listing };
                        if (booking.listing.gender) {
                            translatedBooking.listing.gender = await translateText(booking.listing.gender, 'AR', 'EN');
                        }
                        if (booking.listing.discount) {
                            translatedBooking.listing.discount = await translateText(booking.listing.discount, 'AR', 'EN');
                        }
                        if (booking.listing.name) {
                            translatedBooking.listing.name = await translateText(booking.listing.name, 'AR', 'EN');
                        }
                        if (booking.listing.description) {
                            translatedBooking.listing.description = await translateText(booking.listing.description, 'AR', 'EN');
                        }
                        if (Array.isArray(booking.listing.agegroup)) {
                            translatedBooking.listing.agegroup = await translateArrayFields(booking.listing.agegroup, 'AR', 'EN');
                        }
                        if (Array.isArray(booking.listing.location)) {
                            translatedBooking.listing.location = await translateArrayFields(booking.listing.location, 'AR', 'EN');
                        }
                        if (Array.isArray(booking.listing.facilities)) {
                            translatedBooking.listing.facilities = await translateArrayFields(booking.listing.facilities, 'AR', 'EN');
                        }
                        if (Array.isArray(booking.listing.operatingHours)) {
                            translatedBooking.listing.operatingHours = await translateArrayFields(booking.listing.operatingHours, 'AR', 'EN');
                        }
                    }
                    
                    // Translate review comment if present
                    if (booking.review && booking.review.comment) {
                        translatedBooking.review = {
                            ...booking.review,
                            comment: await translateText(booking.review.comment, 'AR', 'EN')
                        };
                    }
                    
                    // Translate reward fields if present
                    if (booking.reward) {
                        translatedBooking.reward = {
                            ...booking.reward,
                            description: await translateText(booking.reward.description, 'AR', 'EN'),
                            category: await translateText(booking.reward.category, 'AR', 'EN')
                        };
                    }
                    
                    return translatedBooking;
                }));
                
                if (redisClient.isReady) await redisClient.setEx(cacheKey, AR_CACHE_EXPIRATION, JSON.stringify(translatedBookings));
                return translatedBookings;
            }

            return bookings;
        } catch (error) {
            console.error(`Failed to get bookings for user ${uid}: ${error.message}`);
            throw new Error(`Failed to get bookings for user ${uid}: ${error.message}`);
        }
    },

    // 5. Update Booking
    async updateBooking(id, data, lang = 'en', reqDetails = {}) {
        try {

            const bookingId = parseInt(id);
            const currentBooking = await prisma.booking.findUnique({
                where: { id: bookingId },
                include: { user:true, listing: true }  
            });
            if (!currentBooking) throw new Error('Booking not found');

            let updateData = { ...data };
            if (lang === 'ar' && deeplClient) {
                if (data.additionalNote) updateData.additionalNote = await translateText(data.additionalNote, 'EN-US', 'AR');
                if (data.booking_hours) updateData.booking_hours = await translateText(data.booking_hours, 'EN-US', 'AR');
                if (data.ageGroup) updateData.ageGroup = await translateText(data.ageGroup, 'EN-US', 'AR');
                // if(data.status) updateData.status = await translateText(data.status, 'EN-US', 'AR');
                 if(data.paymentMethod) updateData.paymentMethod = await translateText(data.paymentMethod, 'EN-US', 'AR');
                  if (data.status || data.paymentMethod) {
                updateData.status = updateData.status.toUpperCase();
                updateData.paymentMethod = updateData.paymentMethod ? updateData.paymentMethod.toUpperCase() : 'UNPAID';
            }
               
            }
            if (data.bookingDate) updateData.bookingDate = new Date(data.bookingDate);
            if (data.numberOfPersons) updateData.numberOfPersons = parseInt(data.numberOfPersons);

            const updatedBooking = await prisma.booking.update({
                where: { id: bookingId },
                data: updateData,
               include: { user: { select: { id: true, fname: true, lname: true, email: true } }, listing: true, review: { select: { id: true, rating: true, comment: true, createdAt: true } }, reward: true }
            });

            // Handle status and payment updates
            setImmediate(async () => {
                try {
                    // Send email notifications for status updates
                    if (data.status && data.status !== currentBooking.status) {
                        const statusSubject = 'Booking Status Update';
                        const statusMessage = `Hello ${currentBooking.user.fname || 'Customer'},\n\nYour booking for "${currentBooking.listing.name}" has been updated.\n\nNew Status: ${data.status}\nBooking Date: ${currentBooking.bookingDate}\nBooking ID: ${bookingId}`;
                        await sendMail(currentBooking.user.email, statusSubject, statusMessage, 'en', {
                            name: currentBooking.user.fname || 'Customer',
                            listingName: currentBooking.listing.name,
                            status: data.status
                        });

                        // Create notification for status update
                        await prisma.notification.create({
                            data: {
                                userId: currentBooking.user.id,
                                title: 'Booking Status Updated',
                                message: `Your booking for ${currentBooking.listing.name} status has been updated to ${data.status}.`,
                                type: 'BOOKING',
                                entityId: bookingId.toString(),
                                entityType: 'Booking'
                            }
                        });
                    }

                    // Send email notifications for payment updates
                    if (data.paymentMethod && data.paymentMethod !== currentBooking.paymentMethod) {
                        const paymentSubject = 'Booking Payment Update';
                        const paymentStatus = data.paymentMethod === 'UNPAID' ? 'Payment Required' : 'Payment Confirmed';
                        const paymentMessage = `Hello ${currentBooking.user.fname || 'Customer'},\n\nYour payment status for booking "${currentBooking.listing.name}" has been updated.\n\nPayment Status: ${paymentStatus}\nPayment Method: ${data.paymentMethod}\nBooking Date: ${currentBooking.bookingDate}\nBooking ID: ${bookingId}\nGuests: ${currentBooking.numberOfPersons || 'N/A'}\nBooking Hours: ${currentBooking.booking_hours || 'N/A'}`;
                        
                        await sendMail(currentBooking.user.email, paymentSubject, paymentMessage, 'en', {
                            name: currentBooking.user.fname || 'Customer',
                            listingName: currentBooking.listing.name,
                            paymentStatus: paymentStatus,
                            paymentMethod: data.paymentMethod
                        });

                        // Create notification for payment update
                        await prisma.notification.create({
                            data: {
                                userId: currentBooking.user.id,
                                title: 'Payment Status Updated',
                                message: `Your payment for ${currentBooking.listing.name} has been updated to ${paymentStatus}.`,
                                type: 'BOOKING',
                                entityId: bookingId.toString(),
                                entityType: 'Booking'
                            }
                        });
                    }

                    // Cache invalidation and updates
                    if (redisClient.isReady) {
                        const keysToDel = [
                            cacheKeys.bookingAr(bookingId), 
                            cacheKeys.userBookingsAr(currentBooking.user.uid), 
                            cacheKeys.listingBookingsAr(currentBooking.listingId),
                            cacheKeys.userNotificationsAr(currentBooking.user.uid)
                        ];
                        const allBookingsKeys = await redisClient.keys(cacheKeys.allBookingsAr('*'));
                        if (allBookingsKeys.length) keysToDel.push(...allBookingsKeys);
                        if (keysToDel.length > 0) await redisClient.del(keysToDel);

                        // Update individual booking cache with latest data
                        if (deeplClient) {
                            const translatedBooking = await translateBookingFields(updatedBooking, 'AR', 'EN');
                            await redisClient.setEx(cacheKeys.bookingAr(bookingId), AR_CACHE_EXPIRATION, JSON.stringify(translatedBooking));
                        }

                        // Update user bookings cache with latest data
                        const userBookings = await prisma.booking.findMany({
                            where: { user: { uid: currentBooking.user.uid } },
                            include: { listing: true, review: true, reward: true },
                            orderBy: { createdAt: 'desc' }
                        });

                        if (userBookings.length > 0 && deeplClient) {
                            const translatedUserBookings = await Promise.all(
                                userBookings.map(b => translateBookingFields(b, 'AR', 'EN'))
                            );
                            await redisClient.setEx(cacheKeys.userBookingsAr(currentBooking.user.uid), AR_CACHE_EXPIRATION, JSON.stringify(translatedUserBookings));
                        }

                        // Update review cache if this booking has a review
                        if (updatedBooking.review) {
                            const reviewWithIncludes = await prisma.review.findUnique({
                                where: { id: updatedBooking.review.id },
                                include: { 
                                    user: { select: { uid: true, fname: true, lname: true } }, 
                                    listing: { select: { name: true, id: true, description: true, agegroup: true, location: true, facilities: true, operatingHours: true } },
                                    booking: { select: { id: true, bookingDate: true, additionalNote: true, ageGroup: true, status: true, booking_hours: true, paymentMethod: true } }
                                }
                            });
                            
                            if (reviewWithIncludes && deeplClient) {
                                const translatedReview = await translateReviewFields(reviewWithIncludes, 'AR', 'EN');
                                await redisClient.setEx(cacheKeys.reviewAr(updatedBooking.review.id), AR_CACHE_EXPIRATION, JSON.stringify(translatedReview));

                                // Update user reviews cache if affected
                                const userReviews = await prisma.review.findMany({
                                    where: { user: { uid: currentBooking.user.uid } },
                                    include: { 
                                        listing: { select: { name: true, id: true, description: true, agegroup: true, location: true, facilities: true, operatingHours: true } },
                                        booking: { select: { id: true, bookingDate: true, additionalNote: true, ageGroup: true, status: true, booking_hours: true, paymentMethod: true } }
                                    },
                                    orderBy: { createdAt: 'desc' }
                                });

                                if (userReviews.length > 0) {
                                    const translatedUserReviews = await Promise.all(
                                        userReviews.map(r => translateReviewFields(r, 'AR', 'EN'))
                                    );
                                    await redisClient.setEx(cacheKeys.userReviewsAr(currentBooking.user.uid), AR_CACHE_EXPIRATION, JSON.stringify(translatedUserReviews));
                                }
                            }
                        }

                        // Update listing cache if payment or status changed
                        if ((data.status && data.status !== currentBooking.status) || 
                            (data.paymentMethod && data.paymentMethod !== currentBooking.paymentMethod)) {
                            
                            const listingCacheKey = cacheKeys.listingAr(currentBooking.listingId);
                            await redisClient.del(listingCacheKey);
                            
                            const currentListing = await prisma.listing.findUnique({
                                where: { id: currentBooking.listingId },
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
                } catch (bgError) {
                    console.error(`Background task error for booking update ${bookingId}:`, bgError);
                }
            });

            recordAuditLog(AuditLogAction.BOOKING_UPDATED, {
                userId: reqDetails.actorUserId, 
                entityName: 'Booking', 
                entityId: bookingId.toString(),
                oldValues: currentBooking, 
                newValues: updatedBooking, 
                description: `Booking ${bookingId} updated.`,
                ipAddress: reqDetails.ipAddress, 
                userAgent: reqDetails.userAgent,
            });

            if (lang === 'ar' && deeplClient) {
                return await translateBookingFields(updatedBooking, 'AR', 'EN');
            }
            return updatedBooking;
        } catch (error) {
            console.error(`Failed to update booking ${id}: ${error.message}`);
            throw new Error(`Failed to update booking ${id}: ${error.message}`);
        }
    },
    // 6. Delete Booking
    async deleteBooking(id, reqDetails = {}) {
        try {
            const bookingId = parseInt(id);
            const bookingToDelete = await prisma.booking.findUnique({ 
                where: { id: bookingId }, 
                include: { user: true, listing: true, review: true } 
            });
            if (!bookingToDelete) throw new Error('Booking not found');

            // Delete related records and the booking itself
            await prisma.$transaction([
                prisma.reward.deleteMany({ where: { bookingId: bookingId } }),
                prisma.notification.deleteMany({ where: { entityId: id.toString(), entityType: 'Booking' }}),
                // Delete review if exists
                ...(bookingToDelete.review ? [prisma.review.delete({ where: { id: bookingToDelete.review.id } })] : []),
                prisma.booking.delete({ where: { id: bookingId } })
            ]);

            setImmediate(async () => {
                try {
                    // Recalculate reward category after deletion
                    const totalPointsResult = await prisma.reward.aggregate({ 
                        where: { userId: bookingToDelete.userId }, 
                        _sum: { points: true } 
                    });
                    await updateRewardCategory(bookingToDelete.userId, totalPointsResult._sum.points || 0);

                    // Send email notification for booking deletion
                    const deletionSubject = 'Booking Cancelled';
                    const deletionMessage = `Hello ${bookingToDelete.user.fname || 'Customer'},\n\nYour booking for "${bookingToDelete.listing.name}" has been cancelled.\n\nBooking ID: ${bookingId}\nOriginal Booking Date: ${bookingToDelete.bookingDate}`;
                    await sendMail(bookingToDelete.user.email, deletionSubject, deletionMessage, 'en', {
                        name: bookingToDelete.user.fname || 'Customer',
                        listingName: bookingToDelete.listing.name
                    });

                    // Create notification for booking deletion
                    await prisma.notification.create({
                        data: {
                            userId: bookingToDelete.user.id,
                            title: 'Booking Cancelled',
                            message: `Your booking for ${bookingToDelete.listing.name} has been cancelled.`,
                            type: 'BOOKING',
                            entityId: bookingId.toString(),
                            entityType: 'Booking'
                        }
                    });

                    if (redisClient.isReady) {
                        const keysToDel = [
                            cacheKeys.bookingAr(bookingId), 
                            cacheKeys.userBookingsAr(bookingToDelete.user.uid), 
                            cacheKeys.listingBookingsAr(bookingToDelete.listingId),
                            cacheKeys.userNotificationsAr(bookingToDelete.user.uid)
                        ];

                        // If booking had a review, delete review cache too
                        if (bookingToDelete.review) {
                            keysToDel.push(
                                cacheKeys.reviewAr(bookingToDelete.review.id),
                                cacheKeys.userReviewsAr(bookingToDelete.user.uid)
                            );
                        }

                        const allBookingsKeys = await redisClient.keys(cacheKeys.allBookingsAr('*'));
                        if (allBookingsKeys.length) keysToDel.push(...allBookingsKeys);
                        if (keysToDel.length > 0) await redisClient.del(keysToDel);

                        // Update user bookings cache after deletion
                        const userBookings = await prisma.booking.findMany({
                            where: { user: { uid: bookingToDelete.user.uid } },
                            include: { listing: true, review: true, reward: true },
                            orderBy: { createdAt: 'desc' }
                        });

                        if (deeplClient) {
                            const translatedUserBookings = await Promise.all(
                                userBookings.map(b => translateBookingFields(b, 'AR', 'EN'))
                            );
                            await redisClient.setEx(cacheKeys.userBookingsAr(bookingToDelete.user.uid), AR_CACHE_EXPIRATION, JSON.stringify(translatedUserBookings));
                        }

                        // Update user reviews cache if review was deleted
                        if (bookingToDelete.review) {
                            const userReviews = await prisma.review.findMany({
                                where: { user: { uid: bookingToDelete.user.uid } },
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
                                await redisClient.setEx(cacheKeys.userReviewsAr(bookingToDelete.user.uid), AR_CACHE_EXPIRATION, JSON.stringify(translatedUserReviews));
                            }
                        }

                        // Update listing cache after booking deletion
                        if (bookingToDelete.listingId && deeplClient) {
                            const listingCacheKey = cacheKeys.listingAr(bookingToDelete.listingId);
                            await redisClient.del(listingCacheKey);
                            
                            const currentListing = await prisma.listing.findUnique({
                                where: { id: bookingToDelete.listingId },
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
                } catch (bgError) {
                    console.error(`Background task error for booking deletion ${bookingId}:`, bgError);
                }
            });

            recordAuditLog(AuditLogAction.BOOKING_CANCELLED, {
                userId: reqDetails.actorUserId, 
                entityName: 'Booking', 
                entityId: bookingId.toString(),
                oldValues: bookingToDelete, 
                description: `Booking ${bookingId} deleted/cancelled.`,
                ipAddress: reqDetails.ipAddress, 
                userAgent: reqDetails.userAgent,
            });

            return { 
                message: `Booking ${bookingId} and related records deleted successfully.`, 
                deletedBookingId: bookingToDelete.id,
                reviewDeleted: !!bookingToDelete.review
            };
        } catch (error) {
            console.error(`Failed to delete booking ${id}: ${error.message}`);
            throw new Error(`Failed to delete booking ${id}: ${error.message}`);
        }
    }
};

export default bookingService;




















































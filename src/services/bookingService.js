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

async function translateBookingFields(booking, targetLang, sourceLang = null) {
    console.log(`Translating booking fields to ${booking}...`);
    if (!booking) return booking;
    const translatedBooking = { ...booking };
    if (booking.additionalNote) {
        translatedBooking.additionalNote = await translateText(booking.additionalNote, targetLang, sourceLang);
    }
    if (booking.listing) {
        translatedBooking.listing = {
            ...booking.listing,
            name: await translateText(booking.listing.name, targetLang, sourceLang),
            description: await translateText(booking.listing.description, targetLang, sourceLang),
            facilities: booking.listing.facilities ? await Promise.all(booking.listing.facilities.map(f => translateText(f, targetLang, sourceLang))) : [],
            location: booking.listing.location ? await Promise.all(booking.listing.location.map(l => translateText(l, targetLang, sourceLang))) : [],
        };
    }
    if (booking.review) {
        translatedBooking.review = {
            ...booking.review,
            comment: await translateText(booking.review.comment, targetLang, sourceLang)
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
                status: await translateText(booking.status, targetLang, sourceLang),
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
                            include: { user: true, listing: true, review: true, reward: true }
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
                    include: { user: {select: {uid: true, fname: true, lname: true}}, listing: true, review: true, reward: true },
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
                        if (booking.status) {
                            translatedBooking.status = await translateText(booking.status, 'AR', 'EN');
                        }
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
                include: { user: true, listing: true, review: true, reward: true }
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
                if (booking.status) {
                    translatedBooking.status = await translateText(booking.status, 'AR', 'EN');
                }
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
                    if (booking.status) {
                        translatedBooking.status = await translateText(booking.status, 'AR', 'EN');
                    }
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
                include: { user: true, listing: true }  
            });
            if (!currentBooking) throw new Error('Booking not found');

            let updateData = { ...data };
            if (lang === 'ar' && deeplClient) {
                if (data.additionalNote) updateData.additionalNote = await translateText(data.additionalNote, 'EN-US', 'AR');
                if (data.booking_hours) updateData.booking_hours = await translateText(data.booking_hours, 'EN-US', 'AR');
                if (data.ageGroup) updateData.ageGroup = await translateText(data.ageGroup, 'EN-US', 'AR');
                if(data.status) updateData.status = await translateText(data.status, 'EN-US', 'AR');
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
                include: { user: true, listing: true, review: true, reward: true }
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
            const bookingToDelete = await prisma.booking.findUnique({ where: { id: bookingId }, include: { user: true } });
            if (!bookingToDelete) throw new Error('Booking not found');

            // Concurrently delete related records and the booking itself
            await prisma.$transaction([
                prisma.reward.deleteMany({ where: { bookingId: bookingId } }),
                prisma.notification.deleteMany({ where: { entityId: id.toString(), entityType: 'Booking' }}),
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

                    if (redisClient.isReady) {
                        const keysToDel = [
                            cacheKeys.bookingAr(bookingId), 
                            cacheKeys.userBookingsAr(bookingToDelete.user.uid), 
                            cacheKeys.listingBookingsAr(bookingToDelete.listingId),
                            cacheKeys.userNotificationsAr(bookingToDelete.user.uid)
                        ];
                        const allBookingsKeys = await redisClient.keys(cacheKeys.allBookingsAr('*'));
                        if (allBookingsKeys.length) keysToDel.push(...allBookingsKeys);
                        if (keysToDel.length > 0) await redisClient.del(keysToDel);

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
                userId: reqDetails.actorUserId, entityName: 'Booking', entityId: bookingId.toString(),
                oldValues: bookingToDelete, description: `Booking ${bookingId} deleted/cancelled.`,
                ipAddress: reqDetails.ipAddress, userAgent: reqDetails.userAgent,
            });

            return { message: `Booking ${bookingId} and related records deleted successfully.`, deletedBookingId: bookingToDelete.id };
        } catch (error) {
            console.error(`Failed to delete booking ${id}: ${error.message}`);
            throw new Error(`Failed to delete booking ${id}: ${error.message}`);
        }
    }
};

export default bookingService;



























































// import prisma from '../utils/prismaClient.js';
// import { recordAuditLog } from '../utils/auditLogHandler.js';
// import { AuditLogAction } from '@prisma/client';
// import { createClient } from "redis";
// import * as deepl from "deepl-node";
// import { sendMail } from '../utils/mailer.js';

// // --- DeepL Configuration ---
// const DEEPL_AUTH_KEY = process.env.DEEPL_AUTH_KEY || "YOUR_DEEPL_AUTH_KEY_HERE";
// if (DEEPL_AUTH_KEY === "YOUR_DEEPL_AUTH_KEY_HERE") {
//     console.warn("DeepL Auth Key is a placeholder. AR translations may not work. Please configure process.env.DEEPL_AUTH_KEY.");
// }
// const deeplClient = DEEPL_AUTH_KEY !== "YOUR_DEEPL_AUTH_KEY_HERE" ? new deepl.Translator(DEEPL_AUTH_KEY) : null;

// // --- Redis Configuration ---
// const REDIS_URL = process.env.REDIS_URL || "redis://default:YOUR_REDIS_PASSWORD@YOUR_REDIS_HOST:PORT";
// const AR_CACHE_EXPIRATION = 365 * 24 * 60 * 60; // 365 days in seconds

// const redisClient = createClient({
//     url: REDIS_URL,
//     socket: {
//         reconnectStrategy: (retries) => {
//             // If we've tried 3 times, stop retrying.
//             if (retries >= 3) {
//                 return false; 
//             }
//             // Otherwise, wait a bit before trying again.
//             return Math.min(retries * 200, 5000);
//         },
//     },
// });

// redisClient.on('error', (err) => console.error('Redis Cache Error ->', err.message));
// (async () => {
//     try {
//         await redisClient.connect();
//         console.log('Redis Cache Connected successfully.');
//     } catch (err) {
//         console.error('Redis Cache - Could not connect ->', err.message);
//     }
// })();

// const cacheKeys = {
//     bookingAr: (bookingId) => `booking:${bookingId}:ar`,
//     userBookingsAr: (uid) => `user:${uid}:bookings:ar`,
//     userNotificationsAr: (uid) => `user:${uid}:notifications:ar`,
//     allBookingsAr: (filterHash = '') => `bookings:all${filterHash}:ar`,
//     listingBookingsAr: (listingId) => `listing:${listingId}:bookings:ar`,
//     listingAr: (listingId) => `listing:${listingId}:ar`,
// };

// // --- Helper Functions ---
// async function translateText(text, targetLang, sourceLang = null) {
//     if (!deeplClient || !text || typeof text !== 'string') {
//         return text;
//     }
//     try {
//         const result = await deeplClient.translateText(text, sourceLang, targetLang);
//         return result.text;
//     } catch (error) {
//         console.error(`DeepL Translation error: ${error.message}`);
//         return text;
//     }
// }

// async function translateArrayFields(arr, targetLang, sourceLang = null) {
//     if (!arr || !Array.isArray(arr)) return arr;
//     return Promise.all(arr.map(item => translateText(item, targetLang, sourceLang)));
// }

// async function translateObject(obj, fieldsToTranslate, targetLang, sourceLang) {
//     if (!obj) return obj;
//     const translatedObj = { ...obj };
//     for (const field of fieldsToTranslate) {
//         if (translatedObj[field]) {
//             translatedObj[field] = await translateText(translatedObj[field], targetLang, sourceLang);
//         }
//     }
//     return translatedObj;
// }

// async function translateBookingFields(booking, targetLang, sourceLang = null) {
//     if (!booking) return booking;

//     const translatedBooking = await translateObject(
//         booking,
//         ['additionalNote', 'ageGroup', 'status', 'booking_hours', 'paymentMethod'],
//         targetLang,
//         sourceLang
//     );

//     if (booking.listing) {
//         translatedBooking.listing = await translateListingFields(booking.listing, targetLang, sourceLang);
//     }
//     if (booking.review) {
//         translatedBooking.review = await translateObject(booking.review, ['comment'], targetLang, sourceLang);
//     }
//     if (booking.reward) {
//         translatedBooking.reward = await translateObject(booking.reward, ['description', 'category'], targetLang, sourceLang);
//     }

//     return translatedBooking;
// }

// async function translateListingFields(listing, targetLang, sourceLang = null) {
//     if (!listing) return listing;

//     const translatedListing = { ...listing };
    
//     // Translate simple properties
//     Object.assign(translatedListing, await translateObject(listing, ['name', 'description'], targetLang, sourceLang));

//     // Translate array properties
//     const arrayFields = ['agegroup', 'location', 'facilities', 'operatingHours'];
//     for (const field of arrayFields) {
//         if (Array.isArray(listing[field])) {
//             translatedListing[field] = await translateArrayFields(listing[field], targetLang, sourceLang);
//         }
//     }

//     // Translate nested objects in arrays (e.g., categories)
//     const categoryFields = ['selectedMainCategories', 'selectedSubCategories', 'selectedSpecificItems'];
//     for (const field of categoryFields) {
//         if (Array.isArray(listing[field])) {
//             translatedListing[field] = await Promise.all(
//                 listing[field].map(async (item) => ({
//                     ...item,
//                     name: await translateText(item.name, targetLang, sourceLang)
//                 }))
//             );
//         }
//     }
    
//     // Translate other nested objects if they exist
//     if (Array.isArray(listing.reviews)) {
//          translatedListing.reviews = await Promise.all(listing.reviews.map(review => translateBookingFields({review}, targetLang, sourceLang).then(r => r.review)));
//     }
//     if (Array.isArray(listing.bookings)) {
//         translatedListing.bookings = await Promise.all(listing.bookings.map(booking => translateBookingFields(booking, targetLang, sourceLang)));
//     }

//     return translatedListing;
// }

// async function updateRewardCategory(userId, totalPoints) {
//     let category = 'BRONZE';
//     if (totalPoints >= 2500) category = 'PLATINUM';
//     else if (totalPoints >= 2000) category = 'GOLD';
//     else if (totalPoints >= 1000) category = 'SILVER';

//     const currentReward = await prisma.reward.findFirst({ where: { userId }, orderBy: { createdAt: 'desc' } });
//     if (!currentReward || currentReward.category !== category) {
//         await prisma.reward.create({ data: { userId, points: 0, category, description: `Upgraded to ${category} tier.` } });
//         await prisma.notification.create({ data: { userId, title: "Reward Tier Upgraded!", message: `Congratulations! You've been upgraded to the ${category} tier.`, type: 'LOYALTY', entityId: userId.toString(), entityType: 'Reward' } });
//     }
// }

// async function sendBookingEmails(booking, listing, user, lang) {
//     try {
//         const userSubject = lang === 'ar' ? 'تأكيد استلام طلب الحجز' : 'Booking Request Received';
//         const userMessage = lang === 'ar' ? `مرحباً ${user.fname || 'العميل'},\n\nلقد استلمنا طلب الحجز الخاص بك لـ: ${listing.name} وهو الآن قيد المراجعة.\n\nتاريخ الحجز المطلوب: ${booking.bookingDate}` : `Hello ${user.fname || 'Customer'},\n\nWe have received your booking request for: ${listing.name}. It is now pending confirmation.\n\nRequested Booking Date: ${booking.bookingDate}`;
//         await sendMail(user.email, userSubject, userMessage, lang, { name: user.fname || 'Customer', listingName: listing.name });

//         const adminMessage = `Hello Admin,\n\nA new booking requires your confirmation.\n\nListing: ${listing.name}\nCustomer: ${user.fname} ${user.lname} (${user.email})\nBooking Date: ${booking.bookingDate}\nBooking Hours: ${booking.booking_hours || 'N/A'}\nGuests: ${booking.numberOfPersons}`;
//         await sendMail(process.env.EMAIL_USER, 'New Booking - Confirmation Required', adminMessage, 'en', { customerName: `${user.fname} ${user.lname}`, listingName: listing.name });
//     } catch (error) {
//         console.error('Email sending error:', error);
//     }
// }

// function createFilterHash(filters) {
//     const sortedFilters = Object.keys(filters).sort().reduce((result, key) => { result[key] = filters[key]; return result; }, {});
//     return JSON.stringify(sortedFilters);
// }

// // --- Booking Service ---
// const bookingService = {

//     async createBooking(data, userUid, lang = 'en', reqDetails = {}) {
//         const { listingId, bookingDate, booking_hours, additionalNote, numberOfPersons, ageGroup } = data;

//         const user = await prisma.user.findUnique({ where: { uid: userUid } });
//         if (!user) throw new Error('User not found');

//         const listing = await prisma.listing.findUnique({ where: { id: listingId } });
//         if (!listing) throw new Error('Listing not found');

//         let dataForDb = { additionalNote, booking_hours, ageGroup };
//         if (lang === 'ar' && deeplClient) {
//             dataForDb = await translateObject(dataForDb, ['additionalNote', 'booking_hours', 'ageGroup'], 'EN-US', 'AR');
//         }

//         const booking = await prisma.booking.create({
//             data: {
//                 userId: user.id,
//                 listingId: listingId,
//                 bookingDate: bookingDate ? new Date(bookingDate) : null,
//                 booking_hours: dataForDb.booking_hours,
//                 additionalNote: dataForDb.additionalNote,
//                 ageGroup: dataForDb.ageGroup,
//                 numberOfPersons: numberOfPersons ? parseInt(numberOfPersons) : null,
//                 status: 'PENDING',
//                 paymentMethod: 'UNPAID',
//                 updatedAt: new Date(),
//             },
//             include: { user: true, listing: true }
//         });

//         const translatedStatus = lang === 'ar' ? await translateText('PENDING', 'AR', 'EN') : 'PENDING';
//         const translatedPayment = lang === 'ar' ? await translateText('UNPAID', 'AR', 'EN') : 'UNPAID';

//         const immediateResponse = {
//             message: lang === 'ar' ? 'تم استلام طلب الحجز بنجاح وسنعود إليك قريباً بالتأكيد.' : 'Booking request received successfully. We will get back to you with a confirmation shortly.',
//             bookingId: booking.id,
//             listingName: lang === 'ar' ? await translateText(listing.name, 'AR', 'EN') : listing.name,
//             status: translatedStatus,
//             paymentMethod: translatedPayment
//         };

//         setImmediate(async () => {
//             try {
//                 // Background tasks: rewards, notifications, emails, audit log
//                 const newReward = await prisma.reward.create({ data: { userId: user.id, bookingId: booking.id, points: 50, description: `Reward for booking request: ${listing.name}`, category: 'BRONZE' } });
//                 const totalPointsResult = await prisma.reward.aggregate({ where: { userId: user.id }, _sum: { points: true } });
//                 await updateRewardCategory(user.id, totalPointsResult._sum.points || 0);

//                 await prisma.notification.create({ data: { userId: user.id, title: 'Booking Request Received', message: `Your booking for ${listing.name} is pending confirmation.`, type: 'BOOKING', entityId: booking.id.toString(), entityType: 'Booking' } });
//                 await prisma.notification.create({ data: { userId: user.id, title: 'Points Awarded!', message: `You've earned 50 points for your new booking request.`, type: 'LOYALTY', entityId: newReward.id.toString(), entityType: 'Reward' } });
//                 await sendBookingEmails(booking, listing, user, lang);

//                 // ** EFFICIENT CACHE INVALIDATION **
//                 if (redisClient.isReady) {
//                     const keysToDel = [
//                         cacheKeys.userBookingsAr(user.uid),
//                         cacheKeys.listingBookingsAr(listingId),
//                         cacheKeys.listingAr(listingId) // Invalidate the listing to update its booking count
//                     ];
//                     await redisClient.del(keysToDel);
//                 }

//                 recordAuditLog(AuditLogAction.BOOKING_CREATED, {
//                     userId: user.id,
//                     entityName: 'Booking',
//                     entityId: booking.id.toString(),
//                     newValues: booking,
//                     description: `Booking request for '${listing.name}' created by ${user.email}.`,
//                     ipAddress: reqDetails.ipAddress,
//                     userAgent: reqDetails.userAgent,
//                 });

//             } catch (bgError) {
//                 console.error(`Background task error for booking ${booking.id}:`, bgError);
//             }
//         });
        
//         return immediateResponse;
//     },

//     async getAllBookings(filters = {}, lang = 'en') {
//         const { page = 1, limit = 10, ...restFilters } = filters;
//         const pageNum = parseInt(page);
//         const limitNum = parseInt(limit);
//         const skip = (pageNum - 1) * limitNum;
//         const filterHash = createFilterHash({ ...restFilters, page: pageNum, limit: limitNum });
//         const cacheKey = cacheKeys.allBookingsAr(filterHash);

//         if (lang === 'ar' && redisClient.isReady) {
//             const cachedData = await redisClient.get(cacheKey);
//             if (cachedData) return JSON.parse(cachedData);
//         }

//         const whereClause = {
//             ...(restFilters.status && { status: restFilters.status }),
//             ...(restFilters.listingId && { listingId: parseInt(restFilters.listingId) }),
//             ...(restFilters.userId && { userId: parseInt(restFilters.userId) }),
//         };

//         const [bookings, total] = await prisma.$transaction([
//             prisma.booking.findMany({
//                 where: whereClause,
//                 include: { user: { select: { uid: true, fname: true, lname: true } }, listing: true, review: true, reward: true },
//                 orderBy: { createdAt: 'desc' },
//                 skip,
//                 take: limitNum
//             }),
//             prisma.booking.count({ where: whereClause })
//         ]);

//         let result = {
//             bookings,
//             pagination: { total, page: pageNum, limit: limitNum, totalPages: Math.ceil(total / limitNum) }
//         };

//         if (lang === 'ar' && deeplClient) {
//             result.bookings = await Promise.all(bookings.map(booking => translateBookingFields(booking, 'AR', 'EN')));
//             if (redisClient.isReady) await redisClient.setEx(cacheKey, AR_CACHE_EXPIRATION, JSON.stringify(result));
//         }

//         return result;
//     },

//     async getBookingById(id, lang = 'en') {
//         const bookingId = parseInt(id);
//         const cacheKey = cacheKeys.bookingAr(bookingId);

//         if (lang === 'ar' && redisClient.isReady) {
//             const cachedBooking = await redisClient.get(cacheKey);
//             if (cachedBooking) return JSON.parse(cachedBooking);
//         }

//         const booking = await prisma.booking.findUnique({
//             where: { id: bookingId },
//             include: { user: true, listing: true, review: true, reward: true }
//         });

//         if (!booking) return null;

//         if (lang === 'ar' && deeplClient) {
//             const translatedBooking = await translateBookingFields(booking, 'AR', 'EN');
//             if (redisClient.isReady) await redisClient.setEx(cacheKey, AR_CACHE_EXPIRATION, JSON.stringify(translatedBooking));
//             return translatedBooking;
//         }

//         return booking;
//     },

//     async getBookingsByUserUid(uid, lang = 'en') {
//         const cacheKey = cacheKeys.userBookingsAr(uid);

//         if (lang === 'ar' && redisClient.isReady) {
//             const cachedBookings = await redisClient.get(cacheKey);
//             if (cachedBookings) return JSON.parse(cachedBookings);
//         }

//         const bookings = await prisma.booking.findMany({
//             where: { user: { uid: uid } },
//             include: { listing: true, review: true, reward: true },
//             orderBy: { createdAt: 'desc' }
//         });

//         if (lang === 'ar' && deeplClient) {
//             const translatedBookings = await Promise.all(bookings.map(booking => translateBookingFields(booking, 'AR', 'EN')));
//             if (redisClient.isReady) await redisClient.setEx(cacheKey, AR_CACHE_EXPIRATION, JSON.stringify(translatedBookings));
//             return translatedBookings;
//         }

//         return bookings;
//     },

//     async updateBooking(id, data, lang = 'en', reqDetails = {}) {
//         const bookingId = parseInt(id);
//         const currentBooking = await prisma.booking.findUnique({
//             where: { id: bookingId },
//             include: { user: true, listing: true }
//         });
//         if (!currentBooking) throw new Error('Booking not found');

//         let updateData = { ...data };
//         if (lang === 'ar' && deeplClient) {
//             const fieldsToTranslate = ['additionalNote', 'booking_hours', 'ageGroup', 'status', 'paymentMethod'];
//             const translatedData = await translateObject(data, fieldsToTranslate, 'EN-US', 'AR');
//             Object.assign(updateData, translatedData);
//         }
//         if (updateData.status) updateData.status = updateData.status.toUpperCase();
//         if (updateData.paymentMethod) updateData.paymentMethod = updateData.paymentMethod.toUpperCase();
//         if (data.bookingDate) updateData.bookingDate = new Date(data.bookingDate);
//         if (data.numberOfPersons) updateData.numberOfPersons = parseInt(data.numberOfPersons);

//         const updatedBooking = await prisma.booking.update({
//             where: { id: bookingId },
//             data: updateData,
//             include: { user: true, listing: true, review: true, reward: true }
//         });

//         setImmediate(async () => {
//             try {
//                 if (data.status && data.status !== currentBooking.status) {
//                     const subject = 'Booking Status Update';
//                     const message = `Hello ${currentBooking.user.fname || 'Customer'},\n\nYour booking for "${currentBooking.listing.name}" has been updated.\n\nNew Status: ${data.status}`;
//                     await sendMail(currentBooking.user.email, subject, message, 'en', { name: currentBooking.user.fname || 'Customer', listingName: currentBooking.listing.name, status: data.status });
//                     await prisma.notification.create({ data: { userId: currentBooking.user.id, title: 'Booking Status Updated', message: `Your booking status for ${currentBooking.listing.name} is now ${data.status}.`, type: 'BOOKING', entityId: bookingId.toString(), entityType: 'Booking' } });
//                 }
                
//                 // ** EFFICIENT CACHE INVALIDATION **
//                 if (redisClient.isReady) {
//                     const keysToDel = [
//                         cacheKeys.bookingAr(bookingId),
//                         cacheKeys.userBookingsAr(currentBooking.user.uid),
//                         cacheKeys.listingBookingsAr(currentBooking.listingId),
//                         cacheKeys.userNotificationsAr(currentBooking.user.uid),
//                         cacheKeys.listingAr(currentBooking.listingId) // Invalidate listing for booking status changes
//                     ];
//                     await redisClient.del(keysToDel);

//                     // Clear any "all bookings" cache that might be stale now
//                     const allBookingsKeys = await redisClient.keys(cacheKeys.allBookingsAr('*'));
//                     if (allBookingsKeys.length) await redisClient.del(allBookingsKeys);
//                 }

//                 recordAuditLog(AuditLogAction.BOOKING_UPDATED, {
//                     userId: reqDetails.actorUserId,
//                     entityName: 'Booking',
//                     entityId: bookingId.toString(),
//                     oldValues: currentBooking,
//                     newValues: updatedBooking,
//                     description: `Booking ${bookingId} updated.`,
//                     ipAddress: reqDetails.ipAddress,
//                     userAgent: reqDetails.userAgent,
//                 });

//             } catch (bgError) {
//                 console.error(`Background task error for booking update ${bookingId}:`, bgError);
//             }
//         });

//         if (lang === 'ar' && deeplClient) {
//             return await translateBookingFields(updatedBooking, 'AR', 'EN');
//         }
//         return updatedBooking;
//     },

//     async deleteBooking(id, reqDetails = {}) {
//         const bookingId = parseInt(id);
//         const bookingToDelete = await prisma.booking.findUnique({ where: { id: bookingId }, include: { user: true } });
//         if (!bookingToDelete) throw new Error('Booking not found');

//         await prisma.$transaction([
//             prisma.reward.deleteMany({ where: { bookingId: bookingId } }),
//             prisma.notification.deleteMany({ where: { entityId: id.toString(), entityType: 'Booking' } }),
//             prisma.booking.delete({ where: { id: bookingId } })
//         ]);

//         setImmediate(async () => {
//             try {
//                 const totalPointsResult = await prisma.reward.aggregate({ where: { userId: bookingToDelete.userId }, _sum: { points: true } });
//                 await updateRewardCategory(bookingToDelete.userId, totalPointsResult._sum.points || 0);
                
//                 // ** EFFICIENT CACHE INVALIDATION **
//                 if (redisClient.isReady) {
//                     const keysToDel = [
//                         cacheKeys.bookingAr(bookingId),
//                         cacheKeys.userBookingsAr(bookingToDelete.user.uid),
//                         cacheKeys.listingBookingsAr(bookingToDelete.listingId),
//                         cacheKeys.userNotificationsAr(bookingToDelete.user.uid),
//                         cacheKeys.listingAr(bookingToDelete.listingId) // Invalidate listing to update booking count
//                     ];
//                     await redisClient.del(keysToDel);
//                 }

//                 recordAuditLog(AuditLogAction.BOOKING_CANCELLED, {
//                     userId: reqDetails.actorUserId,
//                     entityName: 'Booking',
//                     entityId: bookingId.toString(),
//                     oldValues: bookingToDelete,
//                     description: `Booking ${bookingId} deleted/cancelled.`,
//                     ipAddress: reqDetails.ipAddress,
//                     userAgent: reqDetails.userAgent,
//                 });

//             } catch (bgError) {
//                 console.error(`Background task error for booking deletion ${bookingId}:`, bgError);
//             }
//         });
        
//         return { message: `Booking ${bookingId} and related records deleted successfully.`, deletedBookingId: bookingToDelete.id };
//     }
// };

// export default bookingService;
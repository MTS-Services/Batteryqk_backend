
import prisma from '../utils/prismaClient.js';
import { recordAuditLog } from '../utils/auditLogHandler.js';
import { AuditLogAction } from '@prisma/client';
import { getFileUrl, deleteFile } from '../middlewares/multer.js';
import path from 'path';
import { createClient } from "redis";
import * as deepl from "deepl-node";
import { sendMail } from '../utils/mailer.js';
import axios from 'axios';
import FormData from 'form-data';
import pLimit from 'p-limit';


const translationCache = new Map();
const limit = pLimit(5); // Limit concurrent translations to 5
function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
async function translateText(text, targetLang, sourceLang = null) {
    if (!deeplClient) {
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
            while (retries > 0) {
                try {
                    const res = await deeplClient.translateText(text, sourceLang, targetLang);
                    return res;
                } catch (err) {
                    if (err.message.includes("Too many requests")) {
                        console.warn("DeepL rate limit hit, retrying in 100ms...");
                        await delay(100);
                        retries--;
                    } else {
                        throw err;
                    }
                }
            }
            throw new Error("Failed after multiple retries due to rate limits.");
        });

        console.log(`Translated: "${text}" => "${result.text}"`);
        translationCache.set(cacheKey, result.text);
        return result.text;

    } catch (error) {
        console.error(`DeepL Translation error: ${error.message}`);
        return text; // fallback
    }
}

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
            if (retries >= 3) return false;
            return Math.min(retries * 200, 5000);
        },
    },
});

redisClient.on('error', (err) => console.error('Redis: Listing Cache - Error ->', err.message));

(async () => {
    try {
        await redisClient.connect();
    } catch (err) {
        console.error('Redis: Listing Cache - Could not connect ->', err.message);
    }
})();

const cacheKeys = {
    bookingAr: (bookingId) => `booking:${bookingId}:ar`,
    userBookingsAr: (uid) => `user:${uid}:bookings:ar`,
    listingAr: (listingId) => `listing:${listingId}:ar`,
    allListingsAr: (filterHash = '') => `listings:all${filterHash}:ar`,
    reviewAr: (reviewId) => `review:${reviewId}:ar`,
    userReviewsAr: (uid) => `user:${uid}:reviews:ar`,

};
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
                // DO NOT translate user's proper names. Preserve them.
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

const listingService = {


async createListing(data, files, lang = "en", reqDetails = {}) {
    const { 
        name, price, description, agegroup, location, facilities, operatingHours, 
        mainCategoryIds, subCategoryIds, specificItemIds 
    } = data;

    console.log('Received category data:', {
        mainCategoryIds,
        subCategoryIds,
        specificItemIds
    });

    // --- 1. Prepare Data for Database (store in English by default) ---
    const listingDataForDb = {
        price: price ? parseFloat(price) : null,
        main_image: null, // Will be updated after upload
        sub_images: [], // Will be updated after upload
        name: name || null,
        description: description || null,
        agegroup: agegroup ? (Array.isArray(agegroup) ? agegroup : [agegroup]) : [],
        location: location ? (Array.isArray(location) ? location : [location]) : [],
        facilities: facilities ? (Array.isArray(facilities) ? facilities : [facilities]) : [],
        operatingHours: operatingHours ? (Array.isArray(operatingHours) ? operatingHours : [operatingHours]) : [],
    };

    // If input is Arabic, translate to English for database storage
    if (lang === "ar" && deeplClient) {
        [
            listingDataForDb.name,
            listingDataForDb.description,
            listingDataForDb.agegroup,
            listingDataForDb.location,
            listingDataForDb.facilities,
            listingDataForDb.operatingHours,
        ] = await Promise.all([
            name ? translateText(name, "EN-US", "AR") : null,
            description ? translateText(description, "EN-US", "AR") : null,
            agegroup ? translateArrayFields(Array.isArray(agegroup) ? agegroup : [agegroup], "EN-US", "AR") : [],
            location ? translateArrayFields(Array.isArray(location) ? location : [location], "EN-US", "AR") : [],
            facilities ? translateArrayFields(Array.isArray(facilities) ? facilities : [facilities], "EN-US", "AR") : [],
            operatingHours ? translateArrayFields(Array.isArray(operatingHours) ? operatingHours : [operatingHours], "EN-US", "AR") : [],
        ]);
    }
    
    // --- 2. Connect Category Relationships (FIXED) ---
    const processCategories = (categoryIds) => {
        if (!categoryIds) return [];
        
        // Handle if it's a string (comma-separated)
        if (typeof categoryIds === 'string') {
            return categoryIds.split(',').map(id => parseInt(id.trim())).filter(id => !isNaN(id));
        }
        
        // Handle if it's already an array
        if (Array.isArray(categoryIds)) {
            return categoryIds.map(id => parseInt(id)).filter(id => !isNaN(id));
        }
        
        // Handle single number
        const singleId = parseInt(categoryIds);
        return !isNaN(singleId) ? [singleId] : [];
    };

    const mainCategoryIdsArray = processCategories(mainCategoryIds);
    const subCategoryIdsArray = processCategories(subCategoryIds);
    const specificItemIdsArray = processCategories(specificItemIds);
    
    console.log('Processed category arrays:', {
        mainCategoryIdsArray,
        subCategoryIdsArray,
        specificItemIdsArray
    });

    // Only add connections if arrays are not empty
    if (mainCategoryIdsArray.length > 0) {
        listingDataForDb.selectedMainCategories = {
            connect: mainCategoryIdsArray.map(id => ({ id }))
        };
    }
    
    if (subCategoryIdsArray.length > 0) {
        listingDataForDb.selectedSubCategories = {
            connect: subCategoryIdsArray.map(id => ({ id }))
        };
    }
    
    if (specificItemIdsArray.length > 0) {
        listingDataForDb.selectedSpecificItems = {
            connect: specificItemIdsArray.map(id => ({ id }))
        };
    }

    // --- 3. Create the Listing First ---
    const newListingWithRelations = await prisma.listing.create({
        data: listingDataForDb,
        include: {
            selectedMainCategories: true,
            selectedSubCategories: true,
            selectedSpecificItems: true,
        },
    });

    console.log('Created listing with relations:', {
        id: newListingWithRelations.id,
        mainCategories: newListingWithRelations.selectedMainCategories?.length || 0,
        subCategories: newListingWithRelations.selectedSubCategories?.length || 0,
        specificItems: newListingWithRelations.selectedSpecificItems?.length || 0
    });

    // --- 4. Handle All Background Tasks (INCLUDING IMAGE UPLOAD) ---
        setImmediate(async () => {
            try {
                let finalListing = newListingWithRelations;
                
                // *** IMAGE UPLOAD HAPPENS HERE IN BACKGROUND ***
                if (files && (files.main_image || files.sub_images)) {
                    console.log(`Starting background image upload for listing ${newListingWithRelations.id}`);
                    
                    const uploadResult = await this.uploadImageFromClient(files);
                    
                    if (uploadResult.success) {
                        const updateData = {};
                        
                        if (uploadResult.data.main_image) {
                            updateData.main_image = uploadResult.data.main_image.url;
                        }
                        
                        if (uploadResult.data.sub_images && uploadResult.data.sub_images.length > 0) {
                            updateData.sub_images = uploadResult.data.sub_images.map(img => img.url);
                        }
                        
                        // Update listing with image URLs and get final listing with all relations
                        finalListing = await prisma.listing.update({
                            where: { id: newListingWithRelations.id },
                            data: updateData,
                            include: {
                                selectedMainCategories: true,
                                selectedSubCategories: true,
                                selectedSpecificItems: true,
                                reviews: {
                                    where: { status: 'ACCEPTED' },
                                    select: { rating: true, comment: true, createdAt: true, user: { select: { fname: true, lname: true } } }
                                },
                                bookings: {
                                    select: { id: true, status: true, createdAt: true, user: { select: { fname: true, lname: true } }, bookingDate: true, booking_hours: true, additionalNote: true, ageGroup: true, numberOfPersons: true, paymentMethod: true }
                                }
                            }
                        });
                        
                        console.log(`Background image upload completed for listing ${newListingWithRelations.id}`);
                    } else {
                        console.error(`Background image upload failed for listing ${newListingWithRelations.id}:`, uploadResult.error);
                    }
                }

                // Calculate stats for the final listing
                const acceptedReviews = finalListing.reviews || [];
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

                const enhancedFinalListing = {
                    ...finalListing,
                    averageRating: Math.round(averageRating * 10) / 10,
                    totalReviews,
                    ratingDistribution,
                    totalBookings: (finalListing.bookings || []).length,
                    confirmedBookings: (finalListing.bookings || []).filter(b => b.status === 'CONFIRMED').length
                };

                // Create Arabic cache for individual listing in background
                if (deeplClient && redisClient.isReady) {
                    try {
                        const translatedListing = await translateListingFields(enhancedFinalListing, "AR", "EN");
                        await redisClient.setEx(
                            cacheKeys.listingAr(enhancedFinalListing.id),
                            AR_CACHE_EXPIRATION,
                            JSON.stringify(translatedListing)
                        );
                        console.log(`Redis: AR Cache - Cached new listing ${enhancedFinalListing.id} in Arabic`);
                    } catch (cacheError) {
                        console.error(`Redis: AR Cache - Error caching new listing ${enhancedFinalListing.id} ->`, cacheError.message);
                    }

                    // Clear all listings cache to force refresh
                    try {
                        const keys = await redisClient.keys(cacheKeys.allListingsAr('*'));
                        if (keys.length > 0) {
                            await redisClient.del(keys);
                            console.log(`Redis: AR Cache - Cleared all listings cache after new listing creation`);
                        }
                    } catch (clearError) {
                        console.error(`Redis: AR Cache - Error clearing all listings cache ->`, clearError.message);
                    }
                }

                // Handle notifications and emails
                const allUsers = await prisma.user.findMany({
                    select: { id: true, email: true, fname: true },
                });

                const notificationPromises = allUsers.map(user => 
                    prisma.notification.create({
                        data: {
                            userId: user.id,
                            title: "New Listing Available",
                            message: `A new listing "${enhancedFinalListing.name || 'Untitled'}" has been added.`,
                            type: 'GENERAL',
                            entityId: enhancedFinalListing.id.toString(),
                            entityType: 'Listing'
                        }
                    })
                );
                await Promise.all(notificationPromises);

                const listingDetails = `
    Name: ${enhancedFinalListing.name || 'N/A'}
    Price: ${enhancedFinalListing.price ? `$${enhancedFinalListing.price}` : 'N/A'}
    Description: ${enhancedFinalListing.description || 'No description available.'}
    Location: ${enhancedFinalListing.location?.join(', ') || 'N/A'}
    Facilities: ${enhancedFinalListing.facilities?.join(', ') || 'N/A'}
    Main Categories: ${enhancedFinalListing.selectedMainCategories?.map(cat => cat.name).join(', ') || 'N/A'}
    Sub Categories: ${enhancedFinalListing.selectedSubCategories?.map(cat => cat.name).join(', ') || 'N/A'}
    Specific Items: ${enhancedFinalListing.selectedSpecificItems?.map(item => item.name).join(', ') || 'N/A'}
                `.trim();

                const emailPromises = allUsers.map(user => sendMail(
                    user.email,
                    "New Listing Available - Full Details",
                    `Hello ${user.fname || 'there'},\n\nA new listing has been added. Here are the details:\n\n${listingDetails}\n\nBest regards,\nBatteryqk team`,
                    "en",
                    { name: user.fname || 'there', listingDetails: listingDetails }
                ).catch(err => console.error(`Failed to send email to ${user.email}:`, err)));
                
                await Promise.allSettled(emailPromises);
                console.log(`Background tasks completed for new listing ${enhancedFinalListing.id}`);

            } catch (error) {
                console.error(`Error in background task for listing ${newListingWithRelations.id}:`, error);
            }

            recordAuditLog(AuditLogAction.LISTING_CREATED, {
                userId: reqDetails.actorUserId,
                entityName: 'Listing',
                entityId: newListingWithRelations.id,
                newValues: newListingWithRelations,
                description: `Listing '${newListingWithRelations.name || newListingWithRelations.id}' created.`,
                ipAddress: reqDetails.ipAddress,
                userAgent: reqDetails.userAgent,
            });
        });

    // --- 5. Return Response Immediately ---
    return {
        success: true,
        message: lang === "ar" ? "تم إنشاء القائمة بنجاح." : "Listing created successfully. Images are being uploaded in background.",
        listing: lang === "ar" ? {
            ...newListingWithRelations,
            name: data.name,
            description: data.description,
            agegroup: data.agegroup || [],
            location: data.location || [],
            facilities: data.facilities || [],
            operatingHours: data.operatingHours || [],
        } : newListingWithRelations
    };
},

uploadImageFromClient: async function(files) {
    try {
        const formData = new FormData();
        
        // Handle main image
        if (files.main_image && files.main_image[0]) {
            const mainFile = files.main_image[0];
            if (mainFile.buffer) {
                formData.append('main_image', mainFile.buffer, {
                    filename: mainFile.originalname,
                    contentType: mainFile.mimetype,
                });
            } else {
                formData.append('main_image', mainFile);
            }
        }
        
        // Handle sub images with same name as main image
        if (files.sub_images && files.main_image && files.main_image[0]) {
            const mainImageName = files.main_image[0].originalname;
            const mainImageBaseName = mainImageName.split('.')[0];
            
            files.sub_images.forEach((subFile, index) => {
                const extension = subFile.originalname.split('.').pop();
                const newName = `${mainImageBaseName}_sub_${index + 1}.${extension}`;
                
                if (subFile.buffer) {
                    formData.append('sub_images', subFile.buffer, {
                        filename: newName,
                        contentType: subFile.mimetype,
                    });
                } else {
                    formData.append('sub_images', subFile, newName);
                }
            });
        }
        
        console.log('Uploading images with main image name base');

        const response = await axios.post(
            'http://u8c040k0kgggogk8g0coosww-032347850067:3001/upload',
            formData,
            {
                headers: formData.getHeaders(),
                maxContentLength: Infinity,
                maxBodyLength: Infinity,
            }
        );
        
        return {
            success: true,
            data: response.data
        };
    } catch (error) {
        console.error('Upload failed:', error.response?.data || error.message);
        return {
            success: false,
            error: error.message
        };
    }
},




// uploadImageFromClient: async function (files) {
//     try {
//         const form = new FormData();

//         // Append main image
//         if (files.main_image?.[0]) {
//             const file = files.main_image[0];
//             form.append('main_image', file.buffer, {
//                 filename: file.originalname,
//                 contentType: file.mimetype,
//             });
//         }

//         // Append sub images
//         if (files.sub_images && Array.isArray(files.sub_images)) {
//             files.sub_images.forEach((subFile) => {
//                 form.append('sub_images', subFile.buffer, {
//                     filename: subFile.originalname,
//                     contentType: subFile.mimetype,
//                 });
//             });
//         }

//         const response = await axios.post(
//             'http://q0c040w8s4gcc40kso48cog0-082014034375:3001/upload',
//             form,
//             {
//                 headers: form.getHeaders(),
//                 maxContentLength: Infinity,
//                 maxBodyLength: Infinity,
//             }
//         );

//         return {
//             success: true,
//             data: response.data
//         };
//     } catch (error) {
//         console.error('Upload failed:', error.response?.data || error.message);
//         return {
//             success: false,
//             error: error.message
//         };
//     }

// },


async getAllListings(filters = {}, lang = "en") {
    const { page = 1, limit = 8, search, rating, ...otherFilters } = filters;
    const pageNum = parseInt(page, 10) || 1;
    const limitNum = parseInt(limit, 10) || 8;
    const offset = (pageNum - 1) * limitNum;

    // Build where clause for filtering
    let whereClause = { isActive: true };
    let usingSimilaritySearch = false;

    // Search functionality - enhanced to include facilities and multiple field search
    if (search) {
        const searchTerms = Array.isArray(search) ? search : [search];
        whereClause.OR = [
            { name: { contains: search, mode: 'insensitive' } },
            { description: { contains: search, mode: 'insensitive' } },
            { location: { hasSome: searchTerms } },
            { facilities: { hasSome: searchTerms } }
        ];
    }

    // Filter by main categories
    if (otherFilters.mainCategoryIds && otherFilters.mainCategoryIds.length > 0) {
        whereClause.selectedMainCategories = {
            some: {
                id: { in: otherFilters.mainCategoryIds.map(id => parseInt(id)) }
            }
        };
    }

    // Filter by sub categories
    if (otherFilters.subCategoryIds && otherFilters.subCategoryIds.length > 0) {
        whereClause.selectedSubCategories = {
            some: {
                id: { in: otherFilters.subCategoryIds.map(id => parseInt(id)) }
            }
        };
    }

    // Filter by specific items
    if (otherFilters.specificItemIds && otherFilters.specificItemIds.length > 0) {
        whereClause.selectedSpecificItems = {
            some: {
                id: { in: otherFilters.specificItemIds.map(id => parseInt(id)) }
            }
        };
    }

    // Price range filter
    if (otherFilters.minPrice || otherFilters.maxPrice || otherFilters.price) {
        whereClause.price = {};
        if (otherFilters.minPrice) whereClause.price.gte = parseFloat(otherFilters.minPrice);
        if (otherFilters.maxPrice) whereClause.price.lte = parseFloat(otherFilters.maxPrice);
        if (otherFilters.price) whereClause.price.equals = parseFloat(otherFilters.price);
    }

    // Location filter
    if (otherFilters.location) {
        const locations = Array.isArray(otherFilters.location) ? otherFilters.location : [otherFilters.location];
        whereClause.location = { hasSome: locations };
    }

    // Enhanced Age group filter - handle "25+ years" format
    if (otherFilters.agegroup && otherFilters.agegroup.length > 0) {
        const ageGroups = Array.isArray(otherFilters.agegroup) ? otherFilters.agegroup : [otherFilters.agegroup];
        
        // Process age groups to handle "25+ years" format
        const processedAgeGroups = [];
        
        ageGroups.forEach(ageGroup => {
            const ageStr = ageGroup.toString().toLowerCase();
            
            // Handle "25+ years" or "25+" format
            const plusMatch = ageStr.match(/(\d+)\+\s*years?/);
            if (plusMatch) {
                const minAge = parseInt(plusMatch[1]);
                // Add common age range patterns that would include this minimum age
                processedAgeGroups.push(ageGroup); // Keep original format
                processedAgeGroups.push(`${minAge}+ year`); // Alternative format without 's'
                processedAgeGroups.push(`${minAge}+ years`); // With 's'
                
                // Also check for ranges that include this age (e.g., "20-25 years" includes "25+")
                const rangeMatch = ageStr.match(/(\d+)-(\d+)\s*years?/);
                if (rangeMatch) {
                    const startAge = parseInt(rangeMatch[1]);
                    const endAge = parseInt(rangeMatch[2]);
                    if (startAge <= minAge && endAge >= minAge) {
                        processedAgeGroups.push(`${startAge}-${endAge} years`); // Keep original range
                    }


                }
                // This will be handled in the similarity search if no exact matches
            } else {
                // Handle regular age ranges like "20-25 years"
                processedAgeGroups.push(ageGroup);
            }
        });
        
        whereClause.agegroup = { hasSome: processedAgeGroups };
    }

    // Get all listing IDs first to check cache
    const allListingIds = await prisma.listing.findMany({
        where: whereClause,
        select: { id: true },
        orderBy: { id: 'asc' }
    });

    let listings = [];
    let totalCount = allListingIds.length;

    // If Arabic language, try to get all listings from Redis cache first
    if (lang === "ar" && deeplClient && redisClient.isReady) {
        const cachedListings = [];
        const uncachedIds = [];

        // Check cache for all listing IDs
        for (const { id } of allListingIds) {
            try {
                const cachedListing = await redisClient.get(cacheKeys.listingAr(id));
                if (cachedListing) {
                    const parsed = JSON.parse(cachedListing);
                    if (parsed.averageRating !== undefined && parsed.totalReviews !== undefined) {
                        cachedListings.push(parsed);
                    } else {
                        uncachedIds.push(id);
                    }
                } else {
                    uncachedIds.push(id);
                }
            } catch (cacheError) {
                console.error(`Redis: AR Cache - Error fetching listing ${id} ->`, cacheError.message);
                uncachedIds.push(id);
            }
        }

        // Fetch uncached listings from database
        let uncachedListings = [];
        if (uncachedIds.length > 0) {
            uncachedListings = await prisma.listing.findMany({
                where: { 
                    id: { in: uncachedIds },
                    ...whereClause 
                },
                include: {
                    selectedMainCategories: true,
                    selectedSubCategories: true,
                    selectedSpecificItems: true,
                    reviews: {
                        where: { status: 'ACCEPTED' },
                        select: { rating: true, comment: true, createdAt: true, user: { select: { fname: true, lname: true } } }
                    },
                    bookings: {
                        select: { id: true, status: true, createdAt: true, user: { select: { fname: true, lname: true } }, status: true, bookingDate: true, booking_hours: true, additionalNote: true, ageGroup: true, numberOfPersons: true, paymentMethod: true }
                    }
                },
                orderBy: { id: 'asc' }
            });

            // Calculate stats and translate uncached listings
            uncachedListings = await Promise.all(
                uncachedListings.map(async (listing) => {
                    const acceptedReviews = listing.reviews;
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

                    const enhancedListing = {
                        ...listing,
                        averageRating: Math.round(averageRating * 10) / 10,
                        totalReviews,
                        ratingDistribution,
                        totalBookings: listing.bookings.length,
                        confirmedBookings: listing.bookings.filter(b => b.status === 'CONFIRMED').length
                    };

                    // Translate and cache
                    const translatedListing = await translateListingFields(enhancedListing, "AR", "EN");
                    
                    try {
                        await redisClient.setEx(
                            cacheKeys.listingAr(listing.id),
                            AR_CACHE_EXPIRATION,
                            JSON.stringify(translatedListing)
                        );
                        console.log(`Redis: AR Cache - Cached individual listing ${listing.id}`);
                    } catch (cacheError) {
                        console.error(`Redis: AR Cache - Error caching listing ${listing.id} ->`, cacheError.message);
                    }

                    return translatedListing;
                })
            );
        }

        // Combine cached and uncached listings
        listings = [...cachedListings, ...uncachedListings].sort((a, b) => a.id - b.id);
        
        console.log(`Redis: AR Cache - Retrieved ${cachedListings.length} from cache, ${uncachedListings.length} from database`);
    } else {
        // English language or cache unavailable - fetch from database
        listings = await prisma.listing.findMany({
            where: whereClause,
            include: {
                selectedMainCategories: true,
                selectedSubCategories: true,
                selectedSpecificItems: true,
                reviews: {
                    where: { status: 'ACCEPTED' },
                    select: { rating: true, comment: true, createdAt: true, user: { select: { fname: true, lname: true } } }
                },
                bookings: {
                    select: { id: true, status: true, createdAt: true, user: { select: { fname: true, lname: true } }, status: true, bookingDate: true, booking_hours: true, additionalNote: true, ageGroup: true, numberOfPersons: true, paymentMethod: true }
                }
            },
            orderBy: { id: 'asc' }
        });

        // Calculate stats for non-Arabic listings
        listings = listings.map(listing => {
            const acceptedReviews = listing.reviews;
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

            return {
                ...listing,
                averageRating: Math.round(averageRating * 10) / 10,
                totalReviews,
                ratingDistribution,
                totalBookings: listing.bookings.length,
                confirmedBookings: listing.bookings.filter(b => b.status === 'CONFIRMED').length
            };
        });
    }

    // Apply rating filter if specified
    if (rating) {
        const minRating = parseFloat(rating);
        listings = listings.filter(listing => listing.averageRating >= minRating);
        totalCount = listings.length;
    }

    // If no results found, use intelligent similarity search
    if (listings.length === 0 && this.hasSearchCriteria(filters)) {
        console.log("No exact matches found, attempting intelligent similarity search...");
        usingSimilaritySearch = true;
        
        // Get all active listings for similarity comparison
        const allListings = await prisma.listing.findMany({
            where: { isActive: true },
            include: {
                selectedMainCategories: true,
                selectedSubCategories: true,
                selectedSpecificItems: true,
                reviews: {
                    where: { status: 'ACCEPTED' },
                    select: { rating: true, comment: true, createdAt: true, user: { select: { fname: true, lname: true } } }
                },
                bookings: {
                    select: { id: true, status: true, createdAt: true, user: { select: { fname: true, lname: true } }, status: true, bookingDate: true, booking_hours: true, additionalNote: true, ageGroup: true, numberOfPersons: true, paymentMethod: true }
                }
            },
            orderBy: { id: 'asc' }
        });

        // Calculate similarity scores
        const scoredListings = allListings.map(listing => ({
            ...listing,
            similarityScore: this.calculateIntelligentSimilarityScore(listing, filters)
        }));

        const similarityThreshold = 0.2;
        
        const filteredScoredListings = scoredListings
            .filter(listing => listing.similarityScore > similarityThreshold)
            .sort((a, b) => {
                if (b.similarityScore !== a.similarityScore) {
                    return b.similarityScore - a.similarityScore;
                }
                return a.id - b.id;
            });

        totalCount = filteredScoredListings.length;
        listings = filteredScoredListings.slice(offset, offset + limitNum);
        
        console.log(`Similarity search found ${listings.length} matches out of ${allListings.length} total listings`);
        
        if (listings.length === 0) {
            return {
                listings: [],
                totalCount: 0,
                totalPages: 0,
                currentPage: pageNum,
                hasNextPage: false,
                hasPrevPage: pageNum > 1,
                usingSimilaritySearch: true
            };
        }
    } else {
        // Apply pagination to the results
        listings = listings.slice(offset, offset + limitNum);
    }

    // Calculate final pagination
    const totalPages = Math.ceil(totalCount / limitNum);

    return {
        listings: listings,
        totalCount: totalCount,
        totalPages,
        currentPage: pageNum,
        hasNextPage: pageNum < totalPages,
        hasPrevPage: pageNum > 1,
        usingSimilaritySearch
    };
},



// Check if any search criteria is provided
hasSearchCriteria(filters) {
    return !!(filters.search || filters.facilities || filters.location || 
              filters.agegroup || filters.mainCategoryIds || filters.subCategoryIds || 
              filters.specificItemIds || filters.minPrice || filters.maxPrice);
},

// Enhanced intelligent similarity calculation with all filter types
calculateIntelligentSimilarityScore(listing, filters) {
    let score = 0;
    let maxScore = 0;

    // Text similarity for search terms
    if (filters.search) {
        maxScore += 1;
        const searchLower = filters.search.toLowerCase();
        
        const nameMatch = listing.name?.toLowerCase().includes(searchLower) ? 0.4 : 0;
        const descMatch = listing.description?.toLowerCase().includes(searchLower) ? 0.3 : 0;
        const semanticMatch = this.calculateSemanticSimilarity(searchLower, listing);
        
        score += nameMatch + descMatch + semanticMatch;
    }

    // Facilities matching
    if (filters.facilities) {
        maxScore += 1.2;
        const filterFacilities = Array.isArray(filters.facilities) ? filters.facilities : [filters.facilities];
        
        let facilitiesScore = 0;
        filterFacilities.forEach(filterFacility => {
            const components = this.analyzeFacilityComponents(filterFacility);
            const matchScore = this.matchFacilityComponents(components, listing.facilities || []);
            facilitiesScore += matchScore;
        });
        
        score += (facilitiesScore / filterFacilities.length) * 1.2;
    }

    // Location similarity
    if (filters.location) {
        maxScore += 0.8;
        const filterLocations = Array.isArray(filters.location) ? filters.location : [filters.location];
        const locationScore = this.calculateLocationSimilarity(filterLocations, listing.location || []);
        score += locationScore * 0.8;
    }

    // Enhanced Age group similarity
    if (filters.agegroup) {
        maxScore += 0.9; // Increased weight for age group matching
        const filterAgeGroups = Array.isArray(filters.agegroup) ? filters.agegroup : [filters.agegroup];
        const ageScore = this.calculateAgeGroupSimilarity(filterAgeGroups, listing.agegroup || []);
        score += ageScore * 0.9;
    }

    // Price range similarity - reduce weight for price matching
    if (filters.minPrice || filters.maxPrice || filters.price) {
        maxScore += 0.3; // Reduced from 0.5
        const priceScore = this.calculatePriceSimilarity(filters, listing.price);
        score += priceScore * 0.3;
    }

    // Main category similarity
    if (filters.mainCategoryIds && filters.mainCategoryIds.length > 0) {
        maxScore += 0.6;
        const categoryScore = this.calculateCategorySimilarity(
            filters.mainCategoryIds, 
            listing.selectedMainCategories || []
        );
        score += categoryScore * 0.6;
    }

    // Sub category similarity
    if (filters.subCategoryIds && filters.subCategoryIds.length > 0) {
        maxScore += 0.5;
        const subCategoryScore = this.calculateCategorySimilarity(
            filters.subCategoryIds, 
            listing.selectedSubCategories || []
        );
        score += subCategoryScore * 0.5;
    }

    // Specific items similarity
    if (filters.specificItemIds && filters.specificItemIds.length > 0) {
        maxScore += 0.4;
        const specificScore = this.calculateCategorySimilarity(
            filters.specificItemIds, 
            listing.selectedSpecificItems || []
        );
        score += specificScore * 0.4;
    }

    return maxScore > 0 ? Math.min(score / maxScore, 1) : 0;
},

// Enhanced age group similarity with better "25+" handling
calculateAgeGroupSimilarity(filterAgeGroups, listingAgeGroups) {
    if (!listingAgeGroups || listingAgeGroups.length === 0) return 0;
    
    const parseAgeRange = (ageStr) => {
        const cleanAgeStr = ageStr.toString().toLowerCase().trim();
        
        // Handle "25+ years" or "25+" format
        const plusMatch = cleanAgeStr.match(/(\d+)\+\s*years?/);
        if (plusMatch) {
            const minAge = parseInt(plusMatch[1]);
            return { min: minAge, max: 100, isPlus: true }; // Use 100 as upper bound for "+"
        }
        
        // Handle regular ranges like "20-25 years"
        const rangeMatch = cleanAgeStr.match(/(\d+)[-–](\d+)\s*years?/);
        if (rangeMatch) {
            const min = parseInt(rangeMatch[1]);
            const max = parseInt(rangeMatch[2]);
            return { min, max, isPlus: false };
        }
        
        // Handle single age like "25 years"
        const singleMatch = cleanAgeStr.match(/(\d+)\s*years?/);
        if (singleMatch) {
            const age = parseInt(singleMatch[1]);
            return { min: age, max: age, isPlus: false };
        }
        
        return null;
    };

    const filterRanges = filterAgeGroups.map(parseAgeRange).filter(Boolean);
    const listingRanges = listingAgeGroups.flatMap(ageGroupStr => {
        // Handle comma-separated age groups like "12-16 years, 20-25 years,30+ years"
        return ageGroupStr.split(',').map(ageStr => parseAgeRange(ageStr.trim())).filter(Boolean);
    });
    
    if (filterRanges.length === 0 || listingRanges.length === 0) return 0;

    let bestScore = 0;
    
    filterRanges.forEach(filterRange => {
        listingRanges.forEach(listingRange => {
            let similarity = 0;
            
            // Special handling for "+" ranges
            if (filterRange.isPlus && listingRange.isPlus) {
                // Both are "+" ranges - check if they overlap
                const overlapStart = Math.max(filterRange.min, listingRange.min);
                if (overlapStart <= Math.min(filterRange.max, listingRange.max)) {
                    similarity = 1.0; // Perfect match for overlapping "+" ranges
                }
            } else if (filterRange.isPlus) {
                // Filter is "+" range, listing is regular range
                if (listingRange.max >= filterRange.min) {
                    // If listing's max age is >= filter's min age, it's a match
                    const overlap = Math.min(listingRange.max, filterRange.max) - Math.max(listingRange.min, filterRange.min) + 1;
                    const listingSize = listingRange.max - listingRange.min + 1;
                    similarity = Math.max(0, overlap) / listingSize;
                }
            } else if (listingRange.isPlus) {
                // Listing is "+" range, filter is regular range
                if (filterRange.max >= listingRange.min) {
                    // If filter's max age is >= listing's min age, it's a match
                    const overlap = Math.min(filterRange.max, listingRange.max) - Math.max(filterRange.min, listingRange.min) + 1;
                    const filterSize = filterRange.max - filterRange.min + 1;
                    similarity = Math.max(0, overlap) / filterSize;
                }
            } else {
                // Both are regular ranges
                const overlapStart = Math.max(filterRange.min, listingRange.min);
                const overlapEnd = Math.min(filterRange.max, listingRange.max);
                
                if (overlapStart <= overlapEnd) {
                    const overlapSize = overlapEnd - overlapStart + 1;
                    const filterSize = filterRange.max - filterRange.min + 1;
                    similarity = overlapSize / filterSize;
                }
            }
            
            bestScore = Math.max(bestScore, similarity);
        });
    });
    
    return bestScore;
},

// Calculate price similarity with stricter tolerance
calculatePriceSimilarity(filters, listingPrice) {
    if (!listingPrice) return 0;
    
    const minPrice = filters.minPrice ? parseFloat(filters.minPrice) : 0;
    const maxPrice = filters.maxPrice ? parseFloat(filters.maxPrice) : Infinity;
    const price = filters.price ? parseFloat(filters.price) : null;
    
    // If within range, perfect match
    if (listingPrice >= minPrice && listingPrice <= maxPrice || (price && listingPrice === price)) {
        return 1;
    }
    
    // Calculate similarity based on how close the price is to the range
    let distance = 0;
    if (listingPrice < minPrice) {
        distance = minPrice - listingPrice;
    } else if (listingPrice > maxPrice) {
        distance = listingPrice - maxPrice;
    } else if (price && listingPrice !== price) {
        distance = Math.abs(listingPrice - price);
    }

    // Use stricter tolerance for price matching
    const rangeSize = maxPrice === Infinity ? minPrice : maxPrice - minPrice;
    if (rangeSize <= 0) return 0;
    
    const tolerance = Math.max(rangeSize * 0.15, 50); // Reduced from 30% to 15% tolerance and from $100 to $50
    
    const similarity = Math.exp(-distance / tolerance);
    
    // Only return similarity if it's above 0.7 (stricter price matching)
    return similarity > 0.7 ? similarity : 0;
},

// Calculate category similarity (works for main, sub, and specific categories)
calculateCategorySimilarity(filterCategoryIds, listingCategories) {
    if (!listingCategories || listingCategories.length === 0) return 0;
    
    const listingCategoryIds = listingCategories.map(cat => cat.id.toString());
    const filterIds = filterCategoryIds.map(id => id.toString());
    
    // Calculate Jaccard similarity (intersection over union)
    const intersection = filterIds.filter(id => listingCategoryIds.includes(id)).length;
    const union = new Set([...filterIds, ...listingCategoryIds]).size;
    
    return union > 0 ? intersection / union : 0;
},

// Analyze facility components for intelligent matching
analyzeFacilityComponents(facility) {
    const facilityLower = facility.toLowerCase();
    
    const componentMappings = {
        gym: ['gym', 'fitness', 'workout', 'exercise', 'training'],
        pool: ['pool', 'swimming', 'water', 'aquatic'],
        court: ['court', 'tennis', 'basketball', 'badminton', 'volleyball'],
        field: ['field', 'football', 'soccer', 'cricket', 'rugby'],
        parking: ['parking', 'garage', 'valet'],
        wifi: ['wifi', 'internet', 'broadband', 'connection'],
        ac: ['ac', 'air conditioning', 'climate', 'cooling'],
        playground: ['playground', 'play area', 'kids', 'children'],
        garden: ['garden', 'park', 'outdoor', 'green space'],
        restaurant: ['restaurant', 'cafe', 'dining', 'food', 'kitchen'],
        spa: ['spa', 'massage', 'wellness', 'relaxation']
    };
    
    const components = [];
    
    Object.keys(componentMappings).forEach(key => {
        if (componentMappings[key].some(term => facilityLower.includes(term))) {
            components.push(key);
        }
    });
    
    if (components.length === 0) {
        components.push(...facilityLower.split(/\s+/).filter(word => word.length > 2));
    }
    
    return components;
},

// Match facility components against listing facilities
matchFacilityComponents(components, listingFacilities) {
    if (!listingFacilities || listingFacilities.length === 0) return 0;
    
    const listingFacilitiesFlat = listingFacilities.flatMap(fac => 
        fac.split(',').map(f => f.trim().toLowerCase())
    );
    
    let matchScore = 0;
    const totalComponents = components.length;
    
    components.forEach(component => {
        const directMatch = listingFacilitiesFlat.some(lf => 
            lf.includes(component) || component.includes(lf)
        );
        
        if (directMatch) {
            matchScore += 1;
        } else {
            const bestSimilarity = Math.max(
                ...listingFacilitiesFlat.map(lf => 
                    this.calculateStringSimilarity(component, lf)
                )
            );
            if (bestSimilarity > 0.6) {
                matchScore += bestSimilarity;
            }
        }
    });
    
    return totalComponents > 0 ? matchScore / totalComponents : 0;
},

// Calculate semantic similarity for search terms
calculateSemanticSimilarity(searchTerm, listing) {
    const semanticMappings = {
        'gym': ['fitness', 'workout', 'exercise', 'training', 'health club'],
        'pool': ['swimming', 'water', 'aquatic center', 'swim'],
        'restaurant': ['dining', 'food', 'cafe', 'kitchen', 'eatery'],
        'parking': ['garage', 'valet', 'car park'],
        'spa': ['wellness', 'massage', 'relaxation', 'beauty'],
        'outdoor': ['garden', 'park', 'open air', 'terrace'],
        'kids': ['children', 'family', 'playground', 'child-friendly']
    };
    
    let semanticScore = 0;
    const allText = `${listing.name || ''} ${listing.description || ''} ${listing.facilities?.join(' ') || ''}`.toLowerCase();
    
    Object.keys(semanticMappings).forEach(key => {
        if (searchTerm.includes(key)) {
            const relatedTerms = semanticMappings[key];
            const hasRelatedTerm = relatedTerms.some(term => allText.includes(term));
            if (hasRelatedTerm) {
                semanticScore += 0.2;
            }
        }
    });
    
    return Math.min(semanticScore, 0.3);
},

// Calculate location similarity
calculateLocationSimilarity(filterLocations, listingLocations) {
    if (!listingLocations || listingLocations.length === 0) return 0;
    
    let bestScore = 0;
    
    filterLocations.forEach(filterLoc => {
        listingLocations.forEach(listingLoc => {
            if (listingLoc.toLowerCase().includes(filterLoc.toLowerCase()) || 
                filterLoc.toLowerCase().includes(listingLoc.toLowerCase())) {
                bestScore = Math.max(bestScore, 1);
            } else {
                const similarity = this.calculateStringSimilarity(
                    filterLoc.toLowerCase(), 
                    listingLoc.toLowerCase()
                );
                bestScore = Math.max(bestScore, similarity);
            }
        });
    });
    
    return bestScore;
},

// Levenshtein distance based string similarity
calculateStringSimilarity(str1, str2) {
    const len1 = str1.length;
    const len2 = str2.length;
    const matrix = Array(len2 + 1).fill().map(() => Array(len1 + 1).fill(0));

    for (let i = 0; i <= len1; i++) matrix[0][i] = i;
    for (let j = 0; j <= len2; j++) matrix[j][0] = j;

    for (let j = 1; j <= len2; j++) {
        for (let i = 1; i <= len1; i++) {
            if (str1[i - 1] === str2[j - 1]) {
                matrix[j][i] = matrix[j - 1][i - 1];
            } else {
                matrix[j][i] = Math.min(
                    matrix[j - 1][i] + 1,
                    matrix[j][i - 1] + 1,
                    matrix[j - 1][i - 1] + 1
                );
            }
        }
    }

    const distance = matrix[len2][len1];
    const maxLen = Math.max(len1, len2);
    return maxLen === 0 ? 1 : (maxLen - distance) / maxLen;
},






  async getListingById(id, lang = "en") {
    const listingId = parseInt(id, 10);

    // Check Redis cache for Arabic, but only if the full stats are present
    if (lang === "ar" && redisClient.isReady) {
        try {
            const cachedListing = await redisClient.get(cacheKeys.listingAr(listingId));
            if (cachedListing) {
                const parsed = JSON.parse(cachedListing);
                // Ensure cached version has the stats we need
                if (parsed.averageRating !== undefined && parsed.totalReviews !== undefined) {
                    console.log(`Redis: AR Cache - Fetched listing ${listingId} with stats from cache`);
                    return parsed;
                }
            }
            console.log(`Redis: AR Cache - No valid cache found for listing ${listingId}`);
        } catch (cacheError) {
            console.error(`Redis: AR Cache - Error fetching listing ${listingId} ->`, cacheError.message);
        }
    }

    // Fetch the raw listing with all necessary relations for stat calculation
    const listing = await prisma.listing.findUnique({ 
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
                select: { id: true, status: true, createdAt: true, user: { select: { fname: true, lname: true } }, status: true, bookingDate: true, booking_hours: true, additionalNote: true, ageGroup: true, numberOfPersons: true, paymentMethod: true }
            }
        }
    });

    console.log(listing);

    if (!listing) return null;

    // --- Calculate review and booking statistics ---
    const acceptedReviews = listing.reviews;
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

    const enhancedListing = {
        ...listing,
        averageRating: Math.round(averageRating * 10) / 10,
        totalReviews,
        ratingDistribution,
        totalBookings: listing.bookings.length,
        confirmedBookings: listing.bookings.filter(b => b.status === 'CONFIRMED').length
    };
    // --- End of statistics calculation ---

    // Translate and cache if Arabic
    if (lang === "ar" && deeplClient) {
        const translatedListing = await translateListingFields(enhancedListing, "AR", "EN");
        
        if (redisClient.isReady) {
            try {
                await redisClient.setEx(
                    cacheKeys.listingAr(listingId),
                    AR_CACHE_EXPIRATION,
                    JSON.stringify(translatedListing)
                );
                console.log(`Redis: AR Cache - Cached listing ${listingId} with stats`);
            } catch (cacheError) {
                console.error(`Redis: AR Cache - Error caching listing ${listingId} ->`, cacheError.message);
            }
        }
        return translatedListing;
    }

    return enhancedListing;
},

async updateListing(id, data, files, lang = "en", reqDetails = {}) {
    const listingId = parseInt(id, 10);
    const currentListing = await prisma.listing.findUnique({ 
        where: { id: listingId },
        include: {
            selectedMainCategories: true,
            selectedSubCategories: true,
            selectedSpecificItems: true
        }
    });
    console.log(`Updating listing ${listingId} with data:`, data);
    
    if (!currentListing) return null;

    // Handle case where data might be undefined or empty
    const safeData = data || {};
    const { name, price, description, agegroup, location, facilities, operatingHours, 
            mainCategoryIds, subCategoryIds, specificItemIds } = safeData;
    
    let updateData = {};

    // Handle price
    if (price !== undefined) updateData.price = parseFloat(price);

    // Handle text fields with translation based on input language
    if (lang === "ar" && deeplClient) {
        // If input is Arabic, translate to English for database storage
        if (name !== undefined) updateData.name = await translateText(name, "EN-US", "AR");
        if (description !== undefined) updateData.description = await translateText(description, "EN-US", "AR");
        if (agegroup !== undefined) updateData.agegroup = await translateArrayFields(Array.isArray(agegroup) ? agegroup : [agegroup], "EN-US", "AR");
        if (location !== undefined) updateData.location = await translateArrayFields(Array.isArray(location) ? location : [location], "EN-US", "AR");
        if (facilities !== undefined) updateData.facilities = await translateArrayFields(Array.isArray(facilities) ? facilities : [facilities], "EN-US", "AR");
        if (operatingHours !== undefined) updateData.operatingHours = await translateArrayFields(Array.isArray(operatingHours) ? operatingHours : [operatingHours], "EN-US", "AR");
    } else {
        // If input is English, store directly
        if (name !== undefined) updateData.name = name;
        if (description !== undefined) updateData.description = description;
        if (agegroup !== undefined) updateData.agegroup = Array.isArray(agegroup) ? agegroup : [agegroup];
        if (location !== undefined) updateData.location = Array.isArray(location) ? location : [location];
        if (facilities !== undefined) updateData.facilities = Array.isArray(facilities) ? facilities : [facilities];
        if (operatingHours !== undefined) updateData.operatingHours = Array.isArray(operatingHours) ? operatingHours : [operatingHours];
    }

    // Update listing with basic data first (without images)
    await prisma.listing.update({
        where: { id: listingId },
        data: updateData
    });

    // Handle category connections
    if (mainCategoryIds !== undefined) {
        const mainCategoryIdsArray = Array.isArray(mainCategoryIds) ? mainCategoryIds : [mainCategoryIds];
        await prisma.listing.update({
            where: { id: listingId },
            data: {
                selectedMainCategories: {
                    set: mainCategoryIdsArray.map(id => ({ id: parseInt(id) }))
                }
            }
        });
    }

    if (subCategoryIds !== undefined) {
        const subCategoryIdsArray = Array.isArray(subCategoryIds) ? subCategoryIds : [subCategoryIds];
        await prisma.listing.update({
            where: { id: listingId },
            data: {
                selectedSubCategories: {
                    set: subCategoryIdsArray.map(id => ({ id: parseInt(id) }))
                }
            }
        });
    }

    if (specificItemIds !== undefined) {
        const specificItemIdsArray = Array.isArray(specificItemIds) ? specificItemIds : [specificItemIds];
        await prisma.listing.update({
            where: { id: listingId },
            data: {
                selectedSpecificItems: {
                    set: specificItemIdsArray.map(id => ({ id: parseInt(id) }))
                }
            }
        });
    }

    // Handle background tasks for images and other operations
    setImmediate(async () => {
        try {
            let imageUpdateData = {};
            let currentSubImages = [...(currentListing.sub_images || [])];

            // Handle main image - delete old and upload new if provided
            if (files && files.main_image && files.main_image[0]) {
                // Delete old main image first
                if (currentListing.main_image) {
                    try {
                        const oldMainImageFilename = path.basename(new URL(currentListing.main_image).pathname);
                        const deleteSuccess = await this.deleteImageFromServer(oldMainImageFilename);
                        if (deleteSuccess) {
                            console.log(`Successfully deleted old main image for listing ${listingId}`);
                        } else {
                            console.error(`Failed to delete old main image for listing ${listingId}`);
                        }
                    } catch (err) {
                        console.error(`Error parsing old main image URL for listing ${listingId}:`, err);
                    }
                }

                // Upload new main image
                const mainImageUploadResult = await this.uploadImageFromClient({ main_image: files.main_image });
                if (mainImageUploadResult.success && mainImageUploadResult.data.main_image) {
                    imageUpdateData.main_image = mainImageUploadResult.data.main_image.url;
                    console.log(`Main image updated for listing ${listingId}`);
                } else {
                    console.error(`Main image upload failed for listing ${listingId}:`, mainImageUploadResult.error);
                }

                // Handle removed sub-images - delete all previous sub-images from current listing
                if (currentListing.sub_images && currentListing.sub_images.length > 0 && files.sub_images && files.sub_images.length > 0) {
                    console.log(`Deleting all previous sub-images for listing ${listingId}`);
                    
                    for (const imgUrl of currentListing.sub_images) {
                        try {
                            const filename = path.basename(new URL(imgUrl).pathname);
                            const deleteSuccess = await this.deleteImageFromServer(filename);
                            
                            if (deleteSuccess) {
                                console.log(`Successfully deleted previous sub-image: ${imgUrl}`);
                            } else {
                                console.error(`Failed to delete previous sub-image file: ${filename}`);
                            }
                        } catch (err) {
                            console.error(`Error processing previous sub-image deletion for ${imgUrl}:`, err);
                        }
                    }
                    
                    // Clear the current sub images array
                    currentSubImages = [];
                }

                // Handle new sub-images upload
                if (files && files.sub_images && files.sub_images.length > 0) {
                    const subImageUploadResult = await this.uploadSubImagesOnly(files.sub_images, currentListing.name || `listing_${listingId}`);
                    console.log(`Sub-image upload result for listing ${listingId}:`, subImageUploadResult);
                    
                    if (subImageUploadResult.success && subImageUploadResult.data.sub_images) {
                        const newSubImageUrls = subImageUploadResult.data.sub_images.map(img => img.url);
                        currentSubImages = newSubImageUrls;
                        console.log(`New sub images uploaded for listing ${listingId}`);
                    } else {
                        console.error(`Sub images upload failed for listing ${listingId}:`, subImageUploadResult.error);
                    }
                }

                // Update listing with new image URLs if any changes
                if (Object.keys(imageUpdateData).length > 0 || currentListing.sub_images.length > 0 || (files && files.sub_images)) {
                    imageUpdateData.sub_images = currentSubImages;
                    await prisma.listing.update({
                        where: { id: listingId },
                        data: imageUpdateData
                    });
                    console.log(`Image URLs updated for listing ${listingId}. New sub_images:`, currentSubImages);
                }
            }

            // Get final updated listing with all relations and stats
            const finalListing = await prisma.listing.findUnique({
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
                        select: { id: true, status: true, createdAt: true, user: { select: { fname: true, lname: true } }, bookingDate: true, booking_hours: true, additionalNote: true, ageGroup: true, numberOfPersons: true, paymentMethod: true }
                    }
                }
            });

            // Calculate stats
            const acceptedReviews = finalListing.reviews;
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

            const enhancedFinalListing = {
                ...finalListing,
                averageRating: Math.round(averageRating * 10) / 10,
                totalReviews,
                ratingDistribution,
                totalBookings: finalListing.bookings.length,
                confirmedBookings: finalListing.bookings.filter(b => b.status === 'CONFIRMED').length
            };

            // Clear Redis cache for related entities
            if (redisClient.isReady) {
                const keysToDel = [
                    cacheKeys.listingAr(listingId),
                    // Clear all listings cache
                    ...await redisClient.keys(cacheKeys.allListingsAr('*'))
                ];

                // Clear booking caches that reference this listing
                const allBookingKeys = await redisClient.keys(cacheKeys.bookingAr('*'));
                for (const bookingKey of allBookingKeys) {
                    try {
                        const cachedBooking = await redisClient.get(bookingKey);
                        if (cachedBooking) {
                            const booking = JSON.parse(cachedBooking);
                            if (booking.listingId === listingId) {
                                keysToDel.push(bookingKey);
                            }
                        }
                    } catch (e) {
                        console.error('Error checking booking cache:', e);
                    }
                }

                // Clear user bookings caches for users who have bookings for this listing
                const usersWithBookings = await prisma.user.findMany({
                    where: {
                        bookings: {
                            some: { listingId: listingId }
                        }
                    },
                    select: { uid: true }
                });

                for (const user of usersWithBookings) {
                    keysToDel.push(cacheKeys.userBookingsAr(user.uid));
                }

                // Clear review caches that reference this listing
                const allReviewKeys = await redisClient.keys(cacheKeys.reviewAr('*'));
                for (const reviewKey of allReviewKeys) {
                    try {
                        const cachedReview = await redisClient.get(reviewKey);
                        if (cachedReview) {
                            const review = JSON.parse(cachedReview);
                            if (review.listingId === listingId) {
                                keysToDel.push(reviewKey);
                            }
                        }
                    } catch (e) {
                        console.error('Error checking review cache:', e);
                    }
                }

                // Clear user reviews caches for users who have reviews for this listing
                const usersWithReviews = await prisma.user.findMany({
                    where: {
                        reviews: {
                            some: { listingId: listingId }
                        }
                    },
                    select: { uid: true }
                });

                for (const user of usersWithReviews) {
                    keysToDel.push(cacheKeys.userReviewsAr(user.uid));
                }

                if (keysToDel.length > 0) {
                    await redisClient.del(keysToDel);
                }
                console.log(`Redis: AR Cache - Cleared ${keysToDel.length} related cache keys for listing ${listingId}`);
            }

            // Update Arabic cache for the listing
            if (deeplClient && redisClient.isReady) {
                const translatedListing = await translateListingFields(enhancedFinalListing, "AR", "EN");
                await redisClient.setEx(
                    cacheKeys.listingAr(listingId),
                    AR_CACHE_EXPIRATION,
                    JSON.stringify(translatedListing)
                );
                console.log(`Redis: AR Cache - Updated Arabic cache for listing ${listingId}`);

                // Update booking caches that reference this listing
                const bookingsForThisListing = await prisma.booking.findMany({
                    where: { listingId: listingId },
                    include: { 
                        user: { select: { id: true, fname: true, lname: true, email: true, uid: true } }, 
                        listing: true, 
                        review: { select: { id: true, rating: true, comment: true, createdAt: true } }, 
                        reward: true 
                    }
                });

                for (const booking of bookingsForThisListing) {
                    try {
                        const translatedBooking = await translateBookingFields(booking, 'AR', 'EN');
                        await redisClient.setEx(cacheKeys.bookingAr(booking.id), AR_CACHE_EXPIRATION, JSON.stringify(translatedBooking));
                        
                        // Update user bookings cache
                        if (booking.user.uid) {
                            const userBookings = await prisma.booking.findMany({
                                where: { user: { uid: booking.user.uid } },
                                include: { listing: true, review: true, reward: true },
                                orderBy: { createdAt: 'desc' }
                            });
                            
                            const translatedUserBookings = await Promise.all(
                                userBookings.map(b => translateBookingFields(b, 'AR', 'EN'))
                            );
                            await redisClient.setEx(cacheKeys.userBookingsAr(booking.user.uid), AR_CACHE_EXPIRATION, JSON.stringify(translatedUserBookings));
                        }
                    } catch (e) {
                        console.error(`Error updating booking cache for booking ${booking.id}:`, e);
                    }
                }

                // Update review caches that reference this listing
                const reviewsForThisListing = await prisma.review.findMany({
                    where: { listingId: listingId },
                    include: { 
                        user: { select: { id: true, fname: true, lname: true,email: true } }, 
                        listing: { select: {id: true, name: true,  description: true, agegroup: true, location: true, facilities: true, operatingHours: true } },
                        booking: { select: { id: true, bookingDate: true, additionalNote: true, ageGroup: true, status: true, booking_hours: true, paymentMethod: true } }
                    }
                });

                for (const review of reviewsForThisListing) {
                    try {
                        const translatedReview = await translateReviewFields(review, 'AR', 'EN');
                        await redisClient.setEx(cacheKeys.reviewAr(review.id), AR_CACHE_EXPIRATION, JSON.stringify(translatedReview));
                        
                        // Update user reviews cache
                        if (review.user.uid) {
                            const userReviews = await prisma.review.findMany({
                                where: { user: { uid: review.user.uid } },
                                include: { 
                                    listing: { select: { name: true, id: true, description: true, agegroup: true, location: true, facilities: true, operatingHours: true } },
                                    booking: { select: { id: true, bookingDate: true, additionalNote: true, ageGroup: true, status: true, booking_hours: true, paymentMethod: true } }
                                },
                                orderBy: { createdAt: 'desc' }
                            });
                            
                            const translatedUserReviews = await Promise.all(
                                userReviews.map(r => translateReviewFields(r, 'AR', 'EN'))
                            );
                            await redisClient.setEx(cacheKeys.userReviewsAr(review.user.uid), AR_CACHE_EXPIRATION, JSON.stringify(translatedUserReviews));
                        }
                    } catch (e) {
                        console.error(`Error updating review cache for review ${review.id}:`, e);
                    }
                }
            }

            // Record audit log
            recordAuditLog(AuditLogAction.LISTING_UPDATED, {
                userId: reqDetails.actorUserId,
                entityName: 'Listing',
                entityId: enhancedFinalListing.id,
                oldValues: currentListing,
                newValues: enhancedFinalListing,
                description: `Listing '${enhancedFinalListing.name || enhancedFinalListing.id}' updated.`,
                ipAddress: reqDetails.ipAddress,
                userAgent: reqDetails.userAgent,
            });

            console.log(`Background tasks completed for updated listing ${listingId}`);

        } catch (error) {
            console.error(`Error in background task for listing update ${listingId}:`, error);
        }
    });

    // Return simple success response without data
    return true;
},

// // New helper function to upload sub-images only
uploadSubImagesOnly: async function(subImageFiles, baseName) {
    try {
        const formData = new FormData();
        
        // Handle sub images with custom naming
        subImageFiles.forEach((subFile, index) => {
            const extension = subFile.originalname.split('.').pop();
            const newName = `${baseName}_sub_${Date.now()}_${index + 1}.${extension}`;
            
            if (subFile.buffer) {
                formData.append('sub_images', subFile.buffer, {
                    filename: newName,
                    contentType: subFile.mimetype,
                });
            } else {
                formData.append('sub_images', subFile, newName);
            }
        });
        
        console.log('Uploading sub-images only');

        const response = await axios.post(
            'http://u8c040k0kgggogk8g0coosww-032347850067:3001/upload',
            formData,
            {
                headers: formData.getHeaders(),
                maxContentLength: Infinity,
                maxBodyLength: Infinity,
            }
        );
        
        return {
            success: true,
            data: response.data
        };
    } catch (error) {
        console.error('Sub-images upload failed:', error.response?.data || error.message);
        return {
            success: false,
            error: error.message
        };
    }
},

// Helper function to delete image from server
deleteImageFromServer: async function(filename) {
    try {
        console.log(`Attempting to delete image: ${filename}`);
        const deleteUrl = `http://u8c040k0kgggogk8g0coosww-032347850067:3001/delete/${filename}`;
        console.log(`Delete URL: ${deleteUrl}`);
        
        const response = await fetch(deleteUrl, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        console.log(`Delete response for ${filename}:`, result);
        
        if (response.ok && result.success) {
            console.log(`Successfully deleted image: ${filename}`);
            return true;
        } else {
            console.error(`Failed to delete image ${filename}:`, result.error || 'Unknown error');
            return false;
        }
    } catch (error) {
        console.error(`Error deleting image ${filename}:`, error.message);
        return false;
    }
},




async deleteListing(id, reqDetails = {}) {
    const listingId = parseInt(id, 10);
    const listing = await prisma.listing.findUnique({ where: { id: listingId }});
    if (!listing) return null;

    // Delete the listing from database first
    const deletedListing = await prisma.listing.delete({ where: { id: listingId } });

    // Handle background tasks for image deletion and cache clearing
    setImmediate(async () => {
        try {
            // Delete associated images from server
            if (listing.main_image) {
                try {
                    const mainImageFilename = path.basename(new URL(listing.main_image).pathname);
                    const deleteSuccess = await this.deleteImageFromServer(mainImageFilename);
                    if (deleteSuccess) {
                        console.log(`Successfully deleted main image for listing ${listingId}`);
                    } else {
                        console.error(`Failed to delete main image for listing ${listingId}`);
                    }
                } catch (err) {
                    console.error(`Error parsing main image URL for listing ${listingId}:`, err);
                }
            }
            
            if (listing.sub_images && listing.sub_images.length > 0) {
                for (const imageUrl of listing.sub_images) {
                    try {
                        const filename = path.basename(new URL(imageUrl).pathname);
                        const deleteSuccess = await this.deleteImageFromServer(filename);
                        if (deleteSuccess) {
                            console.log(`Successfully deleted sub-image: ${imageUrl}`);
                        } else {
                            console.error(`Failed to delete sub-image: ${filename}`);
                        }
                    } catch (err) {
                        console.error(`Error processing sub-image deletion for ${imageUrl}:`, err);
                    }
                }
            }

            // Clear Redis cache for related entities
            if (redisClient.isReady) {
                const keysToDel = [
                    cacheKeys.listingAr(listingId),
                    // Clear all listings cache
                    ...await redisClient.keys(cacheKeys.allListingsAr('*'))
                ];

                // Clear booking caches that reference this listing
                const allBookingKeys = await redisClient.keys(cacheKeys.bookingAr('*'));
                for (const bookingKey of allBookingKeys) {
                    try {
                        const cachedBooking = await redisClient.get(bookingKey);
                        if (cachedBooking) {
                            const booking = JSON.parse(cachedBooking);
                            if (booking.listingId === listingId) {
                                keysToDel.push(bookingKey);
                            }
                        }
                    } catch (e) {
                        console.error('Error checking booking cache:', e);
                    }
                }

                // Clear user bookings caches for users who had bookings for this listing
                const usersWithBookings = await prisma.user.findMany({
                    where: {
                        bookings: {
                            some: { listingId: listingId }
                        }
                    },
                    select: { uid: true }
                }).catch(() => []); // Handle case where listing is already deleted

                for (const user of usersWithBookings) {
                    keysToDel.push(cacheKeys.userBookingsAr(user.uid));
                }

                // Clear review caches that reference this listing
                const allReviewKeys = await redisClient.keys(cacheKeys.reviewAr('*'));
                for (const reviewKey of allReviewKeys) {
                    try {
                        const cachedReview = await redisClient.get(reviewKey);
                        if (cachedReview) {
                            const review = JSON.parse(cachedReview);
                            if (review.listingId === listingId) {
                                keysToDel.push(reviewKey);
                            }
                        }
                    } catch (e) {
                        console.error('Error checking review cache:', e);
                    }
                }

                // Clear user reviews caches for users who had reviews for this listing
                const usersWithReviews = await prisma.user.findMany({
                    where: {
                        reviews: {
                            some: { listingId: listingId }
                        }
                    },
                    select: { uid: true }
                }).catch(() => []); // Handle case where listing is already deleted

                for (const user of usersWithReviews) {
                    keysToDel.push(cacheKeys.userReviewsAr(user.uid));
                }

                if (keysToDel.length > 0) {
                    await redisClient.del(keysToDel);
                }
                console.log(`Redis: AR Cache - Cleared ${keysToDel.length} related cache keys for deleted listing ${listingId}`);
            }

            // Record audit log
            recordAuditLog(AuditLogAction.LISTING_DELETED, {
                userId: reqDetails.actorUserId,
                entityName: 'Listing',
                entityId: listing.id,
                oldValues: listing,
                description: `Listing '${listing.name || listing.id}' deleted.`,
                ipAddress: reqDetails.ipAddress,
                userAgent: reqDetails.userAgent,
            });

            console.log(`Background tasks completed for deleted listing ${listingId}`);

        } catch (error) {
            console.error(`Error in background task for listing deletion ${listingId}:`, error);
        }
    });

    return deletedListing;
},


};

export default listingService;































// import prisma from '../utils/prismaClient.js';
// import { recordAuditLog } from '../utils/auditLogHandler.js';
// import { AuditLogAction } from '@prisma/client';
// import { getFileUrl, deleteFile } from '../middlewares/multer.js';
// import path from 'path';
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
//             if (retries >= 3) return false;
//             return Math.min(retries * 200, 5000);
//         },
//     },
// });

// redisClient.on('error', (err) => console.error('Redis: Listing Cache - Error ->', err.message));

// (async () => {
//     try {
//         await redisClient.connect();
//     } catch (err) {
//         console.error('Redis: Listing Cache - Could not connect ->', err.message);
//     }
// })();

// const cacheKeys = {
//     listingAr: (listingId) => `listing:${listingId}:ar`,
//     allListingsAr: (filterHash = '') => `listings:all${filterHash}:ar`,
// };

// // --- Helper Functions ---
// async function translateText(text, targetLang, sourceLang = null) {
//     if (!deeplClient) {
//         console.warn("DeepL client is not initialized.");
//         return text;
//     }

//     if (!text || typeof text !== 'string') {
//         return text;
//     }

//     try {
//         const result = await deeplClient.translateText(text, sourceLang, targetLang);
//         console.log(`Translated: "${text}" => "${result.text}"`);
//         return result.text;
//     } catch (error) {
//         console.error(`DeepL Translation error: ${error.message}`);
//         return text;
//     }
// }

// async function translateArrayFields(arr, targetLang, sourceLang = null) {
//     if (!arr || !Array.isArray(arr)) return arr;
//     return await Promise.all(arr.map(item => translateText(item, targetLang, sourceLang)));
// }

// async function translateListingFields(listing, targetLang, sourceLang = null) {
//     if (!listing) return listing;

//     const translatedListing = { ...listing };

//     // Translate basic text fields
//     if (listing.name)
//         translatedListing.name = await translateText(listing.name, targetLang, sourceLang);

//     if (listing.description)
//         translatedListing.description = await translateText(listing.description, targetLang, sourceLang);

//     // Translate array fields
//     const arrayFields = ['agegroup', 'location', 'facilities', 'operatingHours'];
//     for (const field of arrayFields) {
//         if (Array.isArray(listing[field])) // Ensure it's an array
//             translatedListing[field] = await translateArrayFields(listing[field], targetLang, sourceLang);
//     }

//     // Translate categories
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

//     // Translate reviews
//     if (Array.isArray(listing.reviews)) {
//         translatedListing.reviews = await Promise.all(
//             listing.reviews.map(async (review) => ({
//                 ...review,
//                 comment: await translateText(review.comment, targetLang, sourceLang),
//                 status: await translateText(review.status, targetLang, sourceLang),
//                 // DO NOT translate user's proper names. Preserve them.
//                 user: review.user
//             }))
//         );
//     }

//     // Translate bookings
//     if (Array.isArray(listing.bookings)) {
//         translatedListing.bookings = await Promise.all(
//             listing.bookings.map(async (booking) => ({
//                 ...booking,
//                 additionalNote: await translateText(booking.additionalNote, targetLang, sourceLang),
//                 ageGroup: await translateText(booking.ageGroup, targetLang, sourceLang),
//                 status: await translateText(booking.status, targetLang, sourceLang),
//                 booking_hours: booking.booking_hours ?
//                     await translateText(booking.booking_hours, targetLang, sourceLang) :
//                     null,
//                 // DO NOT translate user's proper names. Preserve them.
//                 user: await translateText(booking.user.fname, targetLang, sourceLang) + ' ' + await translateText(booking.user.lname, targetLang, sourceLang),
//                 paymentMethod: booking.paymentMethod ? await translateText(booking.paymentMethod, targetLang, sourceLang) : null
//             }))
//         );
//     }

//     return translatedListing;
// }

// function createFilterHash(filters) {
//     const sortedFilters = Object.keys(filters).sort().reduce((result, key) => {
//         result[key] = filters[key];
//         return result;
//     }, {});
//     return JSON.stringify(sortedFilters);
// }

// const listingService = {

//     async createListing(data, files, lang = "en", reqDetails = {}) {
//         const {
//             name,
//             price,
//             description,
//             agegroup,
//             location,
//             facilities,
//             operatingHours,
//             mainCategoryIds,
//             subCategoryIds,
//             specificItemIds
//         } = data;

//         // --- 1. Handle File Uploads ---
//         let mainImageFilename = null;
//         let subImageFilenames = [];
//         if (files) {
//             if (files.main_image && files.main_image[0]) {
//                 mainImageFilename = files.main_image[0].filename;
//             }
//             if (files.sub_images && files.sub_images.length > 0) {
//                 subImageFilenames = files.sub_images.map(file => file.filename);
//             }
//         }

//         // --- 2. Prepare Data for Database (including translations) ---
//         const listingDataForDb = {
//             price: price ? parseFloat(price) : null,
//             main_image: mainImageFilename ? getFileUrl(mainImageFilename) : null,
//             sub_images: subImageFilenames.map(filename => getFileUrl(filename)),
//         };

//         if (lang === "ar" && deeplClient) {
//             [
//                 listingDataForDb.name,
//                 listingDataForDb.description,
//                 listingDataForDb.agegroup,
//                 listingDataForDb.location,
//                 listingDataForDb.facilities,
//                 listingDataForDb.operatingHours,
//             ] = await Promise.all([
//                 name ? translateText(name, "EN-US", "AR") : null,
//                 description ? translateText(description, "EN-US", "AR") : null,
//                 agegroup ? translateArrayFields(Array.isArray(agegroup) ? agegroup : [agegroup], "EN-US", "AR") : [],
//                 location ? translateArrayFields(Array.isArray(location) ? location : [location], "EN-US", "AR") : [],
//                 facilities ? translateArrayFields(Array.isArray(facilities) ? facilities : [facilities], "EN-US", "AR") : [],
//                 operatingHours ? translateArrayFields(Array.isArray(operatingHours) ? operatingHours : [operatingHours], "EN-US", "AR") : [],
//             ]);
//         } else {
//             listingDataForDb.name = name || null;
//             listingDataForDb.description = description || null;
//             listingDataForDb.agegroup = agegroup ? (Array.isArray(agegroup) ? agegroup : [agegroup]) : [];
//             listingDataForDb.location = location ? (Array.isArray(location) ? location : [location]) : [];
//             listingDataForDb.facilities = facilities ? (Array.isArray(facilities) ? facilities : [facilities]) : [];
//             listingDataForDb.operatingHours = operatingHours ? (Array.isArray(operatingHours) ? operatingHours : [operatingHours]) : [];
//         }

//         // --- 3. Connect Category Relationships ---
//         const mainCategoryIdsArray = Array.isArray(mainCategoryIds) ? mainCategoryIds : (mainCategoryIds ? [mainCategoryIds] : []);
//         const subCategoryIdsArray = Array.isArray(subCategoryIds) ? subCategoryIds : (subCategoryIds ? [subCategoryIds] : []);
//         const specificItemIdsArray = Array.isArray(specificItemIds) ? specificItemIds : (specificItemIds ? [specificItemIds] : []);

//         listingDataForDb.selectedMainCategories = {
//             connect: mainCategoryIdsArray.map(id => ({ id: parseInt(id) }))
//         };
//         listingDataForDb.selectedSubCategories = {
//             connect: subCategoryIdsArray.map(id => ({ id: parseInt(id) }))
//         };
//         listingDataForDb.selectedSpecificItems = {
//             connect: specificItemIdsArray.map(id => ({ id: parseInt(id) }))
//         };

//         // --- 4. Create the Listing in a Single, Atomic Operation ---
//         const newListingWithRelations = await prisma.listing.create({
//             data: listingDataForDb,
//             include: {
//                 selectedMainCategories: true,
//                 selectedSubCategories: true,
//                 selectedSpecificItems: true,
//             },
//         });

//         // --- 5. Handle Background Tasks (Notifications, Emails, Auditing) ---
//         setImmediate(async () => {
//             try {
//                 const allUsers = await prisma.user.findMany({
//                     select: { id: true, email: true, fname: true },
//                 });

//                 const notificationPromises = allUsers.map(user =>
//                     prisma.notification.create({
//                         data: {
//                             userId: user.id,
//                             title: "New Listing Available",
//                             message: `A new listing "${newListingWithRelations.name || 'Untitled'}" has been added.`,
//                             type: 'GENERAL',
//                             entityId: newListingWithRelations.id.toString(),
//                             entityType: 'Listing'
//                         }
//                     })
//                 );
//                 await Promise.all(notificationPromises);

//                 const listingDetails = `
// Name: ${newListingWithRelations.name || 'N/A'}
// Price: ${newListingWithRelations.price ? `$${newListingWithRelations.price}` : 'N/A'}
// Description: ${newListingWithRelations.description || 'No description available.'}
// Location: ${newListingWithRelations.location?.join(', ') || 'N/A'}
// Facilities: ${newListingWithRelations.facilities?.join(', ') || 'N/A'}
// Main Categories: ${newListingWithRelations.selectedMainCategories?.map(cat => cat.name).join(', ') || 'N/A'}
// Sub Categories: ${newListingWithRelations.selectedSubCategories?.map(cat => cat.name).join(', ') || 'N/A'}
// Specific Items: ${newListingWithRelations.selectedSpecificItems?.map(item => item.name).join(', ') || 'N/A'}
//                 `.trim();

//                 const emailPromises = allUsers.map(user => sendMail(
//                     user.email,
//                     "New Listing Available - Full Details",
//                     `Hello ${user.fname || 'there'},\n\nA new listing has been added. Here are the details:\n\n${listingDetails}\n\nBest regards,\nYour Team`,
//                     "en", { name: user.fname || 'there', listingDetails: listingDetails }
//                 ).catch(err => console.error(`Failed to send email to ${user.email}:`, err)));

//                 await Promise.allSettled(emailPromises);
//                 console.log(`Background tasks initiated for new listing ${newListingWithRelations.id}`);

//             } catch (error) {
//                 console.error(`Error in background task for listing ${newListingWithRelations.id}:`, error);
//             }

//             recordAuditLog(AuditLogAction.LISTING_CREATED, {
//                 userId: reqDetails.actorUserId,
//                 entityName: 'Listing',
//                 entityId: newListingWithRelations.id,
//                 newValues: newListingWithRelations,
//                 description: `Listing '${newListingWithRelations.name || newListingWithRelations.id}' created.`,
//                 ipAddress: reqDetails.ipAddress,
//                 userAgent: reqDetails.userAgent,
//             });
//         });

//         // --- 6. Prepare and Return Final Response Data ---
//         if (lang === "ar") {
//             return {
//                 ...newListingWithRelations,
//                 name: data.name,
//                 description: data.description,
//                 agegroup: data.agegroup || [],
//                 location: data.location || [],
//                 facilities: data.facilities || [],
//                 operatingHours: data.operatingHours || [],
//             };
//         }

//         return newListingWithRelations;
//     },

//     async getAllListings(filters = {}, lang = "en") {
//         const { page = 1, limit = 8, search, rating, ...otherFilters } = filters;
//         const pageNum = parseInt(page, 10) || 1;
//         const limitNum = parseInt(limit, 10) || 8;
//         const offset = (pageNum - 1) * limitNum;

//         let whereClause = { isActive: true };
//         let usingSimilaritySearch = false;

//         if (search) {
//             const searchTerms = Array.isArray(search) ? search : [search];
//             whereClause.OR = [
//                 { name: { contains: search, mode: 'insensitive' } },
//                 { description: { contains: search, mode: 'insensitive' } },
//                 { location: { hasSome: searchTerms } },
//                 { facilities: { hasSome: searchTerms } }
//             ];
//         }

//         if (otherFilters.mainCategoryIds && otherFilters.mainCategoryIds.length > 0) {
//             whereClause.selectedMainCategories = {
//                 some: { id: { in: otherFilters.mainCategoryIds.map(id => parseInt(id)) } }
//             };
//         }

//         if (otherFilters.subCategoryIds && otherFilters.subCategoryIds.length > 0) {
//             whereClause.selectedSubCategories = {
//                 some: { id: { in: otherFilters.subCategoryIds.map(id => parseInt(id)) } }
//             };
//         }

//         if (otherFilters.specificItemIds && otherFilters.specificItemIds.length > 0) {
//             whereClause.selectedSpecificItems = {
//                 some: { id: { in: otherFilters.specificItemIds.map(id => parseInt(id)) } }
//             };
//         }

//         if (otherFilters.minPrice || otherFilters.maxPrice || otherFilters.price) {
//             whereClause.price = {};
//             if (otherFilters.minPrice) whereClause.price.gte = parseFloat(otherFilters.minPrice);
//             if (otherFilters.maxPrice) whereClause.price.lte = parseFloat(otherFilters.maxPrice);
//             if (otherFilters.price) whereClause.price.equals = parseFloat(otherFilters.price);
//         }

//         if (otherFilters.location) {
//             const locations = Array.isArray(otherFilters.location) ? otherFilters.location : [otherFilters.location];
//             whereClause.location = { hasSome: locations };
//         }

//         if (otherFilters.agegroup && otherFilters.agegroup.length > 0) {
//             const ageGroups = Array.isArray(otherFilters.agegroup) ? otherFilters.agegroup : [otherFilters.agegroup];
//             const processedAgeGroups = [];
//             ageGroups.forEach(ageGroup => {
//                 const ageStr = ageGroup.toString().toLowerCase();
//                 const plusMatch = ageStr.match(/(\d+)\+\s*years?/);
//                 if (plusMatch) {
//                     const minAge = parseInt(plusMatch[1]);
//                     processedAgeGroups.push(ageGroup);
//                     processedAgeGroups.push(`${minAge}+ year`);
//                     processedAgeGroups.push(`${minAge}+ years`);
//                 } else {
//                     processedAgeGroups.push(ageGroup);
//                 }
//             });
//             whereClause.agegroup = { hasSome: processedAgeGroups };
//         }

//         let allListingsWithStats = [];
//         let totalCount = 0;
//         let listings = [];
//         const includeRelations = {
//             selectedMainCategories: true,
//             selectedSubCategories: true,
//             selectedSpecificItems: true,
//             reviews: {
//                 where: { status: 'ACCEPTED' },
//                 select: { rating: true, comment: true, createdAt: true, user: { select: { fname: true, lname: true } } }
//             },
//             bookings: {
//                 select: { id: true, status: true, createdAt: true, user: { select: { fname: true, lname: true } }, bookingDate: true, booking_hours: true, additionalNote: true, ageGroup: true, numberOfPersons: true, paymentMethod: true }
//             }
//         };
        
//         if (rating) {
//             allListingsWithStats = await prisma.listing.findMany({
//                 where: whereClause,
//                 include: includeRelations,
//                 orderBy: { id: 'asc' }
//             });

//             const listingsWithStats = allListingsWithStats.map(listing => {
//                 const acceptedReviews = listing.reviews;
//                 const totalReviews = acceptedReviews.length;
//                 const averageRating = totalReviews > 0 ? acceptedReviews.reduce((sum, review) => sum + review.rating, 0) / totalReviews : 0;
//                 const ratingDistribution = { 5: 0, 4: 0, 3: 0, 2: 0, 1: 0 };
//                 acceptedReviews.forEach(r => { if(ratingDistribution[r.rating] !== undefined) ratingDistribution[r.rating]++; });

//                 return {
//                     ...listing,
//                     averageRating: Math.round(averageRating * 10) / 10,
//                     totalReviews,
//                     ratingDistribution,
//                     totalBookings: listing.bookings.length,
//                     confirmedBookings: listing.bookings.filter(b => b.status === 'CONFIRMED').length
//                 };
//             });

//             const minRating = parseFloat(rating);
//             const filteredListings = listingsWithStats.filter(listing => listing.averageRating >= minRating);
            
//             totalCount = filteredListings.length;
//             listings = filteredListings.slice(offset, offset + limitNum);
//         } else {
//             totalCount = await prisma.listing.count({ where: whereClause });
//             listings = await prisma.listing.findMany({
//                 where: whereClause,
//                 include: includeRelations,
//                 orderBy: { id: 'asc' },
//                 skip: offset,
//                 take: limitNum
//             });
//         }

//         if (listings.length === 0 && this.hasSearchCriteria(filters)) {
//             console.log("No exact matches found, attempting intelligent similarity search...");
//             usingSimilaritySearch = true;
            
//             const allListings = await prisma.listing.findMany({
//                 where: { isActive: true },
//                 include: includeRelations,
//                 orderBy: { id: 'asc' }
//             });

//             const scoredListings = allListings.map(listing => ({
//                 ...listing,
//                 similarityScore: this.calculateIntelligentSimilarityScore(listing, filters)
//             }));

//             const similarityThreshold = 0.2; 
//             const filteredScoredListings = scoredListings
//                 .filter(listing => listing.similarityScore > similarityThreshold)
//                 .sort((a, b) => b.similarityScore - a.similarityScore || a.id - b.id);

//             totalCount = filteredScoredListings.length;
//             listings = filteredScoredListings.slice(offset, offset + limitNum);
            
//             console.log(`Similarity search found ${listings.length} matches out of ${allListings.length} total listings (threshold: ${similarityThreshold})`);
            
//             if (listings.length === 0) {
//                 return {
//                     listings: [], totalCount: 0, totalPages: 0, currentPage: pageNum,
//                     hasNextPage: false, hasPrevPage: pageNum > 1, usingSimilaritySearch: true
//                 };
//             }
//         }

//         if (!rating && !usingSimilaritySearch) {
//             listings = listings.map(listing => {
//                 const acceptedReviews = listing.reviews;
//                 const totalReviews = acceptedReviews.length;
//                 const averageRating = totalReviews > 0 ? acceptedReviews.reduce((sum, review) => sum + review.rating, 0) / totalReviews : 0;
//                 const ratingDistribution = { 5: 0, 4: 0, 3: 0, 2: 0, 1: 0 };
//                 acceptedReviews.forEach(r => { if(ratingDistribution[r.rating] !== undefined) ratingDistribution[r.rating]++; });
                
//                 return {
//                     ...listing,
//                     averageRating: Math.round(averageRating * 10) / 10,
//                     totalReviews,
//                     ratingDistribution,
//                     totalBookings: listing.bookings.length,
//                     confirmedBookings: listing.bookings.filter(b => b.status === 'CONFIRMED').length
//                 };
//             });
//         }

//         if (lang === "ar" && deeplClient) {
//             const cachedArListings = new Map();
//             if (redisClient.isReady) {
//                 try {
//                     for (const listing of listings) {
//                         const cachedListing = await redisClient.get(cacheKeys.listingAr(listing.id));
//                         if (cachedListing) {
//                             const parsed = JSON.parse(cachedListing);
//                             if (parsed.totalReviews === listing.totalReviews && parsed.totalBookings === listing.totalBookings) {
//                                 cachedArListings.set(listing.id, parsed);
//                             }
//                         }
//                     }
//                 } catch (cacheError) {
//                     console.error("Redis: AR Cache - Error checking individual listings ->", cacheError.message);
//                 }
//             }

//             listings = await Promise.all(
//                 listings.map(async (listing) => {
//                     const cachedListing = cachedArListings.get(listing.id);
//                     if (cachedListing) return cachedListing;

//                     const translatedListing = await translateListingFields(listing, "AR", "EN");
                    
//                     if (redisClient.isReady) {
//                         try {
//                             await redisClient.setEx(
//                                 cacheKeys.listingAr(listing.id),
//                                 AR_CACHE_EXPIRATION,
//                                 JSON.stringify(translatedListing)
//                             );
//                             console.log(`Redis: AR Cache - Cached individual listing ${listing.id}`);
//                         } catch (cacheError) {
//                             console.error(`Redis: AR Cache - Error caching listing ${listing.id} ->`, cacheError.message);
//                         }
//                     }
//                     return translatedListing;
//                 })
//             );
//         }

//         const totalPages = Math.ceil(totalCount / limitNum);

//         return {
//             listings: listings,
//             totalCount: totalCount,
//             totalPages,
//             currentPage: pageNum,
//             hasNextPage: pageNum < totalPages,
//             hasPrevPage: pageNum > 1,
//             usingSimilaritySearch
//         };
//     },

//     hasSearchCriteria(filters) {
//         return !!(filters.search || filters.facilities || filters.location || filters.agegroup ||
//             filters.mainCategoryIds || filters.subCategoryIds || filters.specificItemIds ||
//             filters.minPrice || filters.maxPrice);
//     },

//     calculateIntelligentSimilarityScore(listing, filters) {
//         let score = 0;
//         let maxScore = 0;

//         if (filters.search) {
//             maxScore += 1;
//             const searchLower = filters.search.toLowerCase();
//             const nameMatch = listing.name?.toLowerCase().includes(searchLower) ? 0.4 : 0;
//             const descMatch = listing.description?.toLowerCase().includes(searchLower) ? 0.3 : 0;
//             const semanticMatch = this.calculateSemanticSimilarity(searchLower, listing);
//             score += nameMatch + descMatch + semanticMatch;
//         }

//         if (filters.facilities) {
//             maxScore += 1.2;
//             const filterFacilities = Array.isArray(filters.facilities) ? filters.facilities : [filters.facilities];
//             let facilitiesScore = 0;
//             filterFacilities.forEach(filterFacility => {
//                 const components = this.analyzeFacilityComponents(filterFacility);
//                 facilitiesScore += this.matchFacilityComponents(components, listing.facilities || []);
//             });
//             score += (facilitiesScore / filterFacilities.length) * 1.2;
//         }

//         if (filters.location) {
//             maxScore += 0.8;
//             const filterLocations = Array.isArray(filters.location) ? filters.location : [filters.location];
//             score += this.calculateLocationSimilarity(filterLocations, listing.location || []) * 0.8;
//         }

//         if (filters.agegroup) {
//             maxScore += 0.9;
//             const filterAgeGroups = Array.isArray(filters.agegroup) ? filters.agegroup : [filters.agegroup];
//             score += this.calculateAgeGroupSimilarity(filterAgeGroups, listing.agegroup || []) * 0.9;
//         }

//         if (filters.minPrice || filters.maxPrice || filters.price) {
//             maxScore += 0.3;
//             score += this.calculatePriceSimilarity(filters, listing.price) * 0.3;
//         }

//         if (filters.mainCategoryIds && filters.mainCategoryIds.length > 0) {
//             maxScore += 0.6;
//             score += this.calculateCategorySimilarity(filters.mainCategoryIds, listing.selectedMainCategories || []) * 0.6;
//         }

//         if (filters.subCategoryIds && filters.subCategoryIds.length > 0) {
//             maxScore += 0.5;
//             score += this.calculateCategorySimilarity(filters.subCategoryIds, listing.selectedSubCategories || []) * 0.5;
//         }

//         if (filters.specificItemIds && filters.specificItemIds.length > 0) {
//             maxScore += 0.4;
//             score += this.calculateCategorySimilarity(filters.specificItemIds, listing.selectedSpecificItems || []) * 0.4;
//         }

//         return maxScore > 0 ? Math.min(score / maxScore, 1) : 0;
//     },

//     calculateAgeGroupSimilarity(filterAgeGroups, listingAgeGroups) {
//         if (!listingAgeGroups || listingAgeGroups.length === 0) return 0;

//         const parseAgeRange = (ageStr) => {
//             const cleanAgeStr = ageStr.toString().toLowerCase().trim();
//             const plusMatch = cleanAgeStr.match(/(\d+)\+\s*years?/);
//             if (plusMatch) return { min: parseInt(plusMatch[1]), max: 100, isPlus: true };
//             const rangeMatch = cleanAgeStr.match(/(\d+)[-–](\d+)\s*years?/);
//             if (rangeMatch) return { min: parseInt(rangeMatch[1]), max: parseInt(rangeMatch[2]), isPlus: false };
//             const singleMatch = cleanAgeStr.match(/(\d+)\s*years?/);
//             if (singleMatch) { const age = parseInt(singleMatch[1]); return { min: age, max: age, isPlus: false }; }
//             return null;
//         };

//         const filterRanges = filterAgeGroups.map(parseAgeRange).filter(Boolean);
//         const listingRanges = listingAgeGroups.flatMap(ageGroupStr =>
//             ageGroupStr.split(',').map(ageStr => parseAgeRange(ageStr.trim())).filter(Boolean)
//         );

//         if (filterRanges.length === 0 || listingRanges.length === 0) return 0;

//         let bestScore = 0;
//         filterRanges.forEach(filterRange => {
//             listingRanges.forEach(listingRange => {
//                 let similarity = 0;
//                 if (filterRange.isPlus && listingRange.isPlus) {
//                     if (Math.max(filterRange.min, listingRange.min) <= Math.min(filterRange.max, listingRange.max)) similarity = 1.0;
//                 } else if (filterRange.isPlus) {
//                     if (listingRange.max >= filterRange.min) {
//                         const overlap = Math.min(listingRange.max, filterRange.max) - Math.max(listingRange.min, filterRange.min) + 1;
//                         similarity = Math.max(0, overlap) / (listingRange.max - listingRange.min + 1);
//                     }
//                 } else if (listingRange.isPlus) {
//                     if (filterRange.max >= listingRange.min) {
//                         const overlap = Math.min(filterRange.max, listingRange.max) - Math.max(filterRange.min, listingRange.min) + 1;
//                         similarity = Math.max(0, overlap) / (filterRange.max - filterRange.min + 1);
//                     }
//                 } else {
//                     const overlapStart = Math.max(filterRange.min, listingRange.min);
//                     const overlapEnd = Math.min(filterRange.max, listingRange.max);
//                     if (overlapStart <= overlapEnd) {
//                         similarity = (overlapEnd - overlapStart + 1) / (filterRange.max - filterRange.min + 1);
//                     }
//                 }
//                 bestScore = Math.max(bestScore, similarity);
//             });
//         });
//         return bestScore;
//     },

//     calculatePriceSimilarity(filters, listingPrice) {
//         if (!listingPrice) return 0;
//         const minPrice = filters.minPrice ? parseFloat(filters.minPrice) : 0;
//         const maxPrice = filters.maxPrice ? parseFloat(filters.maxPrice) : Infinity;
//         const price = filters.price ? parseFloat(filters.price) : null;
//         if ((listingPrice >= minPrice && listingPrice <= maxPrice) || (price && listingPrice === price)) return 1;
//         let distance = 0;
//         if (listingPrice < minPrice) distance = minPrice - listingPrice;
//         else if (listingPrice > maxPrice) distance = listingPrice - maxPrice;
//         else if (price && listingPrice !== price) distance = Math.abs(listingPrice - price);
//         const rangeSize = maxPrice === Infinity ? minPrice : maxPrice - minPrice;
//         if (rangeSize <= 0) return 0;
//         const tolerance = Math.max(rangeSize * 0.15, 50);
//         const similarity = Math.exp(-distance / tolerance);
//         return similarity > 0.7 ? similarity : 0;
//     },

//     calculateCategorySimilarity(filterCategoryIds, listingCategories) {
//         if (!listingCategories || listingCategories.length === 0) return 0;
//         const listingCategoryIds = listingCategories.map(cat => cat.id.toString());
//         const filterIds = filterCategoryIds.map(id => id.toString());
//         const intersection = filterIds.filter(id => listingCategoryIds.includes(id)).length;
//         const union = new Set([...filterIds, ...listingCategoryIds]).size;
//         return union > 0 ? intersection / union : 0;
//     },

//     analyzeFacilityComponents(facility) {
//         const facilityLower = facility.toLowerCase();
//         const componentMappings = {
//             gym: ['gym', 'fitness', 'workout'], pool: ['pool', 'swimming'], court: ['court', 'tennis', 'basketball'],
//             field: ['field', 'football', 'soccer'], parking: ['parking', 'garage'], wifi: ['wifi', 'internet'],
//             ac: ['ac', 'air conditioning'], playground: ['playground', 'kids'], garden: ['garden', 'park'],
//             restaurant: ['restaurant', 'cafe', 'dining'], spa: ['spa', 'massage', 'wellness']
//         };
//         const components = [];
//         Object.keys(componentMappings).forEach(key => {
//             if (componentMappings[key].some(term => facilityLower.includes(term))) components.push(key);
//         });
//         if (components.length === 0) components.push(...facilityLower.split(/\s+/).filter(word => word.length > 2));
//         return components;
//     },

//     matchFacilityComponents(components, listingFacilities) {
//         if (!listingFacilities || listingFacilities.length === 0) return 0;
//         const listingFacilitiesFlat = listingFacilities.flatMap(fac => fac.split(',').map(f => f.trim().toLowerCase()));
//         let matchScore = 0;
//         components.forEach(component => {
//             if (listingFacilitiesFlat.some(lf => lf.includes(component) || component.includes(lf))) {
//                 matchScore += 1;
//             } else {
//                 const bestSimilarity = Math.max(...listingFacilitiesFlat.map(lf => this.calculateStringSimilarity(component, lf)));
//                 if (bestSimilarity > 0.6) matchScore += bestSimilarity;
//             }
//         });
//         return components.length > 0 ? matchScore / components.length : 0;
//     },

//     calculateSemanticSimilarity(searchTerm, listing) {
//         const semanticMappings = {
//             'gym': ['fitness', 'workout', 'health club'], 'pool': ['swimming', 'aquatic center'], 'restaurant': ['dining', 'food', 'cafe'],
//             'parking': ['garage', 'car park'], 'spa': ['wellness', 'massage'], 'outdoor': ['garden', 'park', 'terrace'], 'kids': ['children', 'family']
//         };
//         let semanticScore = 0;
//         const allText = `${listing.name || ''} ${listing.description || ''} ${listing.facilities?.join(' ') || ''}`.toLowerCase();
//         Object.keys(semanticMappings).forEach(key => {
//             if (searchTerm.includes(key) && semanticMappings[key].some(term => allText.includes(term))) {
//                 semanticScore += 0.2;
//             }
//         });
//         return Math.min(semanticScore, 0.3);
//     },

//     calculateLocationSimilarity(filterLocations, listingLocations) {
//         if (!listingLocations || listingLocations.length === 0) return 0;
//         let bestScore = 0;
//         filterLocations.forEach(filterLoc => {
//             listingLocations.forEach(listingLoc => {
//                 const filterLower = filterLoc.toLowerCase();
//                 const listingLower = listingLoc.toLowerCase();
//                 if (listingLower.includes(filterLower) || filterLower.includes(listingLower)) {
//                     bestScore = Math.max(bestScore, 1);
//                 } else {
//                     bestScore = Math.max(bestScore, this.calculateStringSimilarity(filterLower, listingLower));
//                 }
//             });
//         });
//         return bestScore;
//     },

//     calculateStringSimilarity(str1, str2) {
//         const len1 = str1.length, len2 = str2.length;
//         const matrix = Array(len2 + 1).fill(null).map(() => Array(len1 + 1).fill(0));
//         for (let i = 0; i <= len1; i++) matrix[0][i] = i;
//         for (let j = 0; j <= len2; j++) matrix[j][0] = j;
//         for (let j = 1; j <= len2; j++) {
//             for (let i = 1; i <= len1; i++) {
//                 matrix[j][i] = str1[i - 1] === str2[j - 1] ? matrix[j - 1][i - 1] :
//                     Math.min(matrix[j - 1][i] + 1, matrix[j][i - 1] + 1, matrix[j - 1][i - 1] + 1);
//             }
//         }
//         const distance = matrix[len2][len1];
//         const maxLen = Math.max(len1, len2);
//         return maxLen === 0 ? 1 : (maxLen - distance) / maxLen;
//     },

//     async getListingById(id, lang = "en") {
//         const listingId = parseInt(id, 10);

//         if (lang === "ar" && redisClient.isReady) {
//             try {
//                 const cachedListing = await redisClient.get(cacheKeys.listingAr(listingId));
//                 if (cachedListing) {
//                     const parsed = JSON.parse(cachedListing);
//                     if (parsed.averageRating !== undefined && parsed.totalReviews !== undefined) {
//                         console.log(`Redis: AR Cache - Fetched listing ${listingId} with stats from cache`);
//                         return parsed;
//                     }
//                 }
//             } catch (cacheError) {
//                 console.error(`Redis: AR Cache - Error fetching listing ${listingId} ->`, cacheError.message);
//             }
//         }

//         const listing = await prisma.listing.findUnique({
//             where: { id: listingId },
//             include: {
//                 selectedMainCategories: true,
//                 selectedSubCategories: true,
//                 selectedSpecificItems: true,
//                 reviews: {
//                     where: { status: 'ACCEPTED' },
//                     select: { rating: true, comment: true, createdAt: true, user: { select: { fname: true, lname: true } } }
//                 },
//                 bookings: {
//                     select: { id: true, status: true, createdAt: true, user: { select: { fname: true, lname: true } }, bookingDate: true, booking_hours: true, additionalNote: true, ageGroup: true, numberOfPersons: true, paymentMethod: true }
//                 }
//             }
//         });

//         if (!listing) return null;

//         const acceptedReviews = listing.reviews;
//         const totalReviews = acceptedReviews.length;
//         const averageRating = totalReviews > 0 ? acceptedReviews.reduce((sum, review) => sum + review.rating, 0) / totalReviews : 0;
//         const ratingDistribution = { 5: 0, 4: 0, 3: 0, 2: 0, 1: 0 };
//         acceptedReviews.forEach(r => { if(ratingDistribution[r.rating] !== undefined) ratingDistribution[r.rating]++; });

//         const enhancedListing = {
//             ...listing,
//             averageRating: Math.round(averageRating * 10) / 10,
//             totalReviews,
//             ratingDistribution,
//             totalBookings: listing.bookings.length,
//             confirmedBookings: listing.bookings.filter(b => b.status === 'CONFIRMED').length
//         };

//         if (lang === "ar" && deeplClient) {
//             const translatedListing = await translateListingFields(enhancedListing, "AR", "EN");
//             if (redisClient.isReady) {
//                 try {
//                     await redisClient.setEx(
//                         cacheKeys.listingAr(listingId),
//                         AR_CACHE_EXPIRATION,
//                         JSON.stringify(translatedListing)
//                     );
//                     console.log(`Redis: AR Cache - Cached listing ${listingId} with stats`);
//                 } catch (cacheError) {
//                     console.error(`Redis: AR Cache - Error caching listing ${listingId} ->`, cacheError.message);
//                 }
//             }
//             return translatedListing;
//         }

//         return enhancedListing;
//     },

//     async updateListing(id, data, files, lang = "en", reqDetails = {}) {
//         const listingId = parseInt(id, 10);
//         const currentListing = await prisma.listing.findUnique({
//             where: { id: listingId },
//             include: {
//                 selectedMainCategories: true,
//                 selectedSubCategories: true,
//                 selectedSpecificItems: true
//             }
//         });
//         if (!currentListing) return null;

//         const safeData = data || {};
//         const { name, price, description, agegroup, location, facilities, operatingHours,
//             mainCategoryIds, subCategoryIds, specificItemIds, removed_sub_images } = safeData;
        
//         let updateData = {};

//         if (price !== undefined) updateData.price = parseFloat(price);

//         if (files && files.main_image && files.main_image[0]) {
//             if (currentListing.main_image) {
//                 deleteFile(path.basename(new URL(currentListing.main_image).pathname));
//             }
//             updateData.main_image = getFileUrl(files.main_image[0].filename);
//         }

//         let finalSubImageFilenames = currentListing.sub_images.map(url => path.basename(new URL(url).pathname));
//         if (removed_sub_images) {
//             const imagesToRemove = Array.isArray(removed_sub_images) ? removed_sub_images : [removed_sub_images];
//             imagesToRemove.forEach(imgUrlToRemove => {
//                 const filenameToRemove = path.basename(new URL(imgUrlToRemove).pathname);
//                 if (deleteFile(filenameToRemove)) {
//                     finalSubImageFilenames = finalSubImageFilenames.filter(fn => fn !== filenameToRemove);
//                 }
//             });
//         }
//         if (files && files.sub_images && files.sub_images.length > 0) {
//             finalSubImageFilenames.push(...files.sub_images.map(file => file.filename));
//         }
//         updateData.sub_images = finalSubImageFilenames.map(filename => getFileUrl(filename));

//         if (lang === "ar" && deeplClient) {
//             if (name !== undefined) updateData.name = await translateText(name, "EN-US", "AR");
//             if (description !== undefined) updateData.description = await translateText(description, "EN-US", "AR");
//             if (agegroup !== undefined) updateData.agegroup = await translateArrayFields(Array.isArray(agegroup) ? agegroup : [agegroup], "EN-US", "AR");
//             if (location !== undefined) updateData.location = await translateArrayFields(Array.isArray(location) ? location : [location], "EN-US", "AR");
//             if (facilities !== undefined) updateData.facilities = await translateArrayFields(Array.isArray(facilities) ? facilities : [facilities], "EN-US", "AR");
//             if (operatingHours !== undefined) updateData.operatingHours = await translateArrayFields(Array.isArray(operatingHours) ? operatingHours : [operatingHours], "EN-US", "AR");
//         } else {
//             if (name !== undefined) updateData.name = name;
//             if (description !== undefined) updateData.description = description;
//             if (agegroup !== undefined) updateData.agegroup = Array.isArray(agegroup) ? agegroup : [agegroup];
//             if (location !== undefined) updateData.location = Array.isArray(location) ? location : [location];
//             if (facilities !== undefined) updateData.facilities = Array.isArray(facilities) ? facilities : [facilities];
//             if (operatingHours !== undefined) updateData.operatingHours = Array.isArray(operatingHours) ? operatingHours : [operatingHours];
//         }

//         await prisma.listing.update({
//             where: { id: listingId },
//             data: updateData
//         });

//         if (mainCategoryIds !== undefined) {
//             const ids = Array.isArray(mainCategoryIds) ? mainCategoryIds : [mainCategoryIds];
//             await prisma.listing.update({
//                 where: { id: listingId },
//                 data: { selectedMainCategories: { set: ids.map(id => ({ id: parseInt(id) })) } }
//             });
//         }
//         if (subCategoryIds !== undefined) {
//             const ids = Array.isArray(subCategoryIds) ? subCategoryIds : [subCategoryIds];
//             await prisma.listing.update({
//                 where: { id: listingId },
//                 data: { selectedSubCategories: { set: ids.map(id => ({ id: parseInt(id) })) } }
//             });
//         }
//         if (specificItemIds !== undefined) {
//             const ids = Array.isArray(specificItemIds) ? specificItemIds : [specificItemIds];
//             await prisma.listing.update({
//                 where: { id: listingId },
//                 data: { selectedSpecificItems: { set: ids.map(id => ({ id: parseInt(id) })) } }
//             });
//         }

//         setImmediate(async () => {
//             try {
//                 // Fetch final updated listing for auditing and re-caching
//                 const finalListing = await prisma.listing.findUnique({
//                     where: { id: listingId },
//                     include: {
//                         selectedMainCategories: true,
//                         selectedSubCategories: true,
//                         selectedSpecificItems: true,
//                         reviews: { where: { status: 'ACCEPTED' }, select: { rating: true } },
//                         bookings: { select: { status: true } }
//                     }
//                 });
                
//                 if (!finalListing) return;

//                 const acceptedReviews = finalListing.reviews;
//                 const totalReviews = acceptedReviews.length;
//                 const averageRating = totalReviews > 0 ? acceptedReviews.reduce((sum, review) => sum + review.rating, 0) / totalReviews : 0;
                
//                 const enhancedFinalListing = {
//                     ...finalListing,
//                     averageRating: Math.round(averageRating * 10) / 10,
//                     totalReviews,
//                     totalBookings: finalListing.bookings.length,
//                     confirmedBookings: finalListing.bookings.filter(b => b.status === 'CONFIRMED').length
//                 };
                
//                 // === CORRECTED CACHE LOGIC ===
//                 // Invalidate and then update only the cache for this specific listing.
//                 if (redisClient.isReady) {
//                     await redisClient.del(cacheKeys.listingAr(listingId)); // Invalidate old version
//                     console.log(`Redis: AR Cache - Invalidated cache for updated listing ${listingId}`);
                    
//                     if (deeplClient) {
//                          // Re-cache the newly updated data
//                         const translatedListing = await translateListingFields(enhancedFinalListing, "AR", "EN");
//                         await redisClient.setEx(
//                             cacheKeys.listingAr(listingId),
//                             AR_CACHE_EXPIRATION,
//                             JSON.stringify(translatedListing)
//                         );
//                         console.log(`Redis: AR Cache - Re-cached updated Arabic cache for listing ${listingId}`);
//                     }
//                 }
                
//                 recordAuditLog(AuditLogAction.LISTING_UPDATED, {
//                     userId: reqDetails.actorUserId,
//                     entityName: 'Listing',
//                     entityId: enhancedFinalListing.id,
//                     oldValues: currentListing,
//                     newValues: enhancedFinalListing,
//                     description: `Listing '${enhancedFinalListing.name || enhancedFinalListing.id}' updated.`,
//                     ipAddress: reqDetails.ipAddress,
//                     userAgent: reqDetails.userAgent,
//                 });

//             } catch (error) {
//                 console.error(`Error in background task for listing update ${listingId}:`, error);
//             }
//         });

//         return true;
//     },

//     async deleteListing(id, reqDetails = {}) {
//         const listingId = parseInt(id, 10);
//         const listing = await prisma.listing.findUnique({ where: { id: listingId } });
//         if (!listing) return null;

//         if (listing.main_image) {
//             deleteFile(path.basename(new URL(listing.main_image).pathname));
//         }
//         if (listing.sub_images && listing.sub_images.length > 0) {
//             listing.sub_images.forEach(imageUrl => {
//                 deleteFile(path.basename(new URL(imageUrl).pathname));
//             });
//         }

//         const deletedListing = await prisma.listing.delete({ where: { id: listingId } });

//         // === CORRECTED CACHE LOGIC ===
//         // Only delete the cache for the specific listing that was removed.
//         if (redisClient.isReady) {
//             try {
//                 await redisClient.del(cacheKeys.listingAr(listingId));
//                 console.log(`Redis: AR Cache - Deleted individual listing ${listingId} from cache`);
//             } catch (cacheError) {
//                 console.error(`Redis: AR Cache - Error deleting listing ${listingId} ->`, cacheError.message);
//             }
//         }

//         recordAuditLog(AuditLogAction.LISTING_DELETED, {
//             userId: reqDetails.actorUserId,
//             entityName: 'Listing',
//             entityId: listing.id,
//             oldValues: listing,
//             description: `Listing '${listing.name || listing.id}' deleted.`,
//             ipAddress: reqDetails.ipAddress,
//             userAgent: reqDetails.userAgent,
//         });

//         return deletedListing;
//     },
// };

// export default listingService;
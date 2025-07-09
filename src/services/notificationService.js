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

redisClient.on('error', (err) => console.error('Redis: Notification Cache - Error ->', err.message));
(async () => {
    try {
        await redisClient.connect();
        console.log('Redis: Notification Cache - Connected successfully.');
    } catch (err) {
        console.error('Redis: Notification Cache - Could not connect ->', err.message);
    }
})();

const cacheKeys = {
    notificationAr: (notificationId) => `notification:${notificationId}:ar`,
    userNotificationsAr: (uid) => `user:${uid}:notifications:ar`,
    allNotificationsAr: (filterHash = '') => `notifications:all${filterHash}:ar`,
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



async function translateNotificationFields(notification, targetLang, sourceLang = null) {
    if (!notification) return notification;
    
    const translatedNotification = { ...notification };
    
    // Translate notification fields
    if (notification.title) {
        translatedNotification.title = await translateText(notification.title, targetLang, sourceLang);
    }
    if (notification.message) {
        translatedNotification.message = await translateText(notification.message, targetLang, sourceLang);
    }
    if (notification.type) {
        translatedNotification.type = await translateText(notification.type, targetLang, sourceLang);
    }
    if (notification.entityType) {
        translatedNotification.entityType = await translateText(notification.entityType, targetLang, sourceLang);
    }
    
    // Translate user fields if present
    if (notification.user) {
        translatedNotification.user = {
            ...notification.user,
            fname: await translateText(notification.user.fname, targetLang, sourceLang),
            lname: await translateText(notification.user.lname, targetLang, sourceLang)
        };
    }
    
    return translatedNotification;
}

function createFilterHash(filters) {
    const sortedFilters = Object.keys(filters).sort().reduce((result, key) => {
        result[key] = filters[key];
        return result;
    }, {});
    return JSON.stringify(sortedFilters);
}

// --- Notification Service ---
const notificationService = {

    // 1. Get All Notifications with Filters
    async getAllNotifications(filters = {}, lang = 'en') {
        try {
            const { page = 1, limit = 20, ...restFilters } = filters;
            const pageNum = parseInt(page);
            const limitNum = parseInt(limit);
            const skip = (pageNum - 1) * limitNum;

            // Build where clause from filters
            const whereClause = {
                ...(restFilters.type && { type: restFilters.type }),
                ...(restFilters.isRead !== undefined && { isRead: restFilters.isRead === 'true' }),
                ...(restFilters.userId && { userId: parseInt(restFilters.userId) }),
            };

            // Get notification IDs from database first
            const [notificationIds, total] = await prisma.$transaction([
                prisma.notification.findMany({
                    where: whereClause,
                    select: { id: true },
                    orderBy: { createdAt: 'desc' },
                    skip,
                    take: limitNum
                }),
                prisma.notification.count({ where: whereClause })
            ]);

            console.log('Database notification IDs found:', notificationIds.length);

            if (notificationIds.length === 0) {
                return {
                    notifications: [],
                    pagination: { total: 0, page: pageNum, limit: limitNum, totalPages: 0 },
                    message: lang === 'ar' ? 'لا توجد إشعارات متاحة' : 'No notifications available'
                };
            }

            const notifications = [];
            const missingNotificationIds = [];

            // Try to get each notification from individual cache entries (Arabic only)
            if (lang === 'ar' && redisClient.isReady) {
                for (const { id } of notificationIds) {
                    const cacheKey = cacheKeys.notificationAr(id);
                    const cachedNotification = await redisClient.get(cacheKey);
                    
                    if (cachedNotification) {
                        notifications.push(JSON.parse(cachedNotification));
                    } else {
                        missingNotificationIds.push(id);
                    }
                }
                console.log(`Found ${notifications.length} cached notifications, ${missingNotificationIds.length} missing from cache`);
            } else {
                missingNotificationIds.push(...notificationIds.map(n => n.id));
            }

            // Fetch missing notifications from database
            if (missingNotificationIds.length > 0) {
                const missingNotifications = await prisma.notification.findMany({
                    where: { id: { in: missingNotificationIds } },
                    include: { 
                        user: { select: { uid: true, fname: true, lname: true, email: true } }
                    },
                    orderBy: { createdAt: 'desc' }
                });

                // Process missing notifications (translate if needed and cache individually)
                for (const notification of missingNotifications) {
                    let processedNotification = notification;

                    if (lang === 'ar' && deeplClient) {
                        const translatedNotification = await translateNotificationFields(notification, 'AR', 'EN');
                        processedNotification = translatedNotification;

                        // Cache the translated notification individually
                        if (redisClient.isReady) {
                            const cacheKey = cacheKeys.notificationAr(notification.id);
                            await redisClient.setEx(cacheKey, AR_CACHE_EXPIRATION, JSON.stringify(translatedNotification));
                        }
                    }

                    notifications.push(processedNotification);
                }
            }

            // Sort notifications to maintain original order (by creation date desc)
            const sortedNotifications = notifications.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

            return {
                notifications: sortedNotifications,
                pagination: { total, page: pageNum, limit: limitNum, totalPages: Math.ceil(total / limitNum) }
            };
        } catch (error) {
            console.error(`Failed to get all notifications: ${error.message}`);
            throw new Error(`Failed to get all notifications: ${error.message}`);
        }
    },

    // 2. Get Notifications by User UID
    async getNotificationsByUserUid(uid, filters = {}, lang = 'en') {
        try {
            const { page = 1, limit = 20, ...restFilters } = filters;
            const pageNum = parseInt(page);
            const limitNum = parseInt(limit);
            const skip = (pageNum - 1) * limitNum;

            // Find user by UID first
            const user = await prisma.user.findUnique({ where: { uid: uid } });
            if (!user) throw new Error('User not found');

            // Build where clause for database query
            const whereClause = {
                userId: user.id,
                ...(restFilters.type && { type: restFilters.type }),
                ...(restFilters.isRead !== undefined && { isRead: restFilters.isRead === 'true' }),
            };

            // Get notification IDs from database first
            const [notificationIds, total] = await prisma.$transaction([
                prisma.notification.findMany({
                    where: whereClause,
                    select: { id: true },
                    orderBy: { createdAt: 'desc' },
                    skip,
                    take: limitNum
                }),
                prisma.notification.count({ where: whereClause })
            ]);

            console.log(`Database notification IDs found for user ${uid}:`, notificationIds.length);

            if (notificationIds.length === 0) {
                return {
                    notifications: [],
                    pagination: { total: 0, page: pageNum, limit: limitNum, totalPages: 0 },
                    message: lang === 'ar' ? 'لا توجد إشعارات متاحة' : 'No notifications available'
                };
            }

            const notifications = [];
            const missingNotificationIds = [];

            // Try to get each notification from individual cache entries (Arabic only)
            if (lang === 'ar' && redisClient.isReady) {
                for (const { id } of notificationIds) {
                    const cacheKey = cacheKeys.notificationAr(id);
                    const cachedNotification = await redisClient.get(cacheKey);
                    
                    if (cachedNotification) {
                        notifications.push(JSON.parse(cachedNotification));
                    } else {
                        missingNotificationIds.push(id);
                    }
                }
                console.log(`Found ${notifications.length} cached notifications for user ${uid}, ${missingNotificationIds.length} missing from cache`);
            } else {
                missingNotificationIds.push(...notificationIds.map(n => n.id));
            }

            // Fetch missing notifications from database
            if (missingNotificationIds.length > 0) {
                const missingNotifications = await prisma.notification.findMany({
                    where: { id: { in: missingNotificationIds } },
                    include: { 
                        user: { select: { uid: true, fname: true, lname: true, email: true } }
                    },
                    orderBy: { createdAt: 'desc' }
                });

                // Process missing notifications (translate if needed and cache individually)
                for (const notification of missingNotifications) {
                    let processedNotification = notification;

                    if (lang === 'ar' && deeplClient) {
                        const translatedNotification = await translateNotificationFields(notification, 'AR', 'EN');
                        processedNotification = translatedNotification;

                        // Cache the translated notification individually
                        if (redisClient.isReady) {
                            const cacheKey = cacheKeys.notificationAr(notification.id);
                            await redisClient.setEx(cacheKey, AR_CACHE_EXPIRATION, JSON.stringify(translatedNotification));
                        }
                    }

                    notifications.push(processedNotification);
                }
            }

            // Sort notifications to maintain original order (by creation date desc)
            const sortedNotifications = notifications.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

            return {
                notifications: sortedNotifications,
                pagination: { total, page: pageNum, limit: limitNum, totalPages: Math.ceil(total / limitNum) },
                source: missingNotificationIds.length > 0 ? 'database+cache' : 'cache'
            };
        } catch (error) {
            console.error(`Failed to get notifications for user ${uid}: ${error.message}`);
            throw new Error(`Failed to get notifications for user ${uid}: ${error.message}`);
        }
    },

    // 3. Get Notification by ID
    async getNotificationById(id, lang = 'en') {
        try {
            const notificationId = parseInt(id);
            const cacheKey = cacheKeys.notificationAr(notificationId);

            if (lang === 'ar' && redisClient.isReady) {
                const cachedNotification = await redisClient.get(cacheKey);
                if (cachedNotification) return JSON.parse(cachedNotification);
            }

            const notification = await prisma.notification.findUnique({
                where: { id: notificationId },
                include: { 
                    user: { select: { uid: true, fname: true, lname: true, email: true } }
                }
            });

            if (!notification) return null;

            if (lang === 'ar' && deeplClient) {
                const translatedNotification = await translateNotificationFields(notification, 'AR', 'EN');
                
                if (redisClient.isReady) {
                    await redisClient.setEx(cacheKey, AR_CACHE_EXPIRATION, JSON.stringify(translatedNotification));
                }
                return translatedNotification;
            }

            return notification;
        } catch (error) {
            console.error(`Failed to get notification by ID ${id}: ${error.message}`);
            throw new Error(`Failed to get notification by ID ${id}: ${error.message}`);
        }
    },

    // 4. Create Notification
    async createNotification(data, lang = 'en', reqDetails = {}) {
        try {
            const { userId, title, message, type, link, entityId, entityType } = data;

            // Find user by ID or UID
            let user;
            if (typeof userId === 'string' && userId.length > 10) {
                // Assuming it's a UID
                user = await prisma.user.findUnique({ where: { uid: userId } });
            } else {
                // Assuming it's a numeric user ID
                user = await prisma.user.findUnique({ where: { id: parseInt(userId) } });
            }

            if (!user) throw new Error('User not found');

            let dataForDb = { title, message };
            if (lang === 'ar' && deeplClient) {
                dataForDb.title = await translateText(title, 'EN-US', 'AR');
                dataForDb.message = await translateText(message, 'EN-US', 'AR');
            }

            const notification = await prisma.notification.create({
                data: {
                    userId: user.id,
                    title: dataForDb.title,
                    message: dataForDb.message,
                    type: type || 'GENERAL',
                    link: link || null,
                    entityId: entityId || null,
                    entityType: entityType || null,
                    isRead: false,
                },
                include: { 
                    user: { select: { uid: true, fname: true, lname: true, email: true } }
                }
            });

            const immediateResponse = lang === 'ar' ? {
                message: 'تم إنشاء الإشعار بنجاح.',
                notificationId: notification.id,
                title: title, // Return original Arabic input
                type: await translateText(type || 'GENERAL', 'AR', 'EN')
            } : {
                message: 'Notification created successfully.',
                notificationId: notification.id,
                title: notification.title,
                type: notification.type
            };

            setImmediate(async () => {
                try {
                    // Clear relevant caches
                    if (redisClient.isReady) {
                        const keysToDel = [
                            cacheKeys.userNotificationsAr(user.uid)
                        ];
                        
                        // Clear all notifications cache
                        const allNotificationsKeys = await redisClient.keys(cacheKeys.allNotificationsAr('*'));
                        if (allNotificationsKeys.length) keysToDel.push(...allNotificationsKeys);
                        
                        if (keysToDel.length > 0) await redisClient.del(keysToDel);

                        // Cache individual notification in Arabic
                        if (deeplClient) {
                            const arNotification = lang === 'ar' ? {
                                ...notification,
                                title: title, // Original Arabic input
                                message: message // Original Arabic input
                            } : await translateNotificationFields(notification, 'AR', 'EN');
                            
                            await redisClient.setEx(
                                cacheKeys.notificationAr(notification.id), 
                                AR_CACHE_EXPIRATION, 
                                JSON.stringify(arNotification)
                            );
                        }
                    }

                    // Send email notification if it's important
                    if (['BOOKING', 'SYSTEM', 'CANCELLATION'].includes(type)) {
                        try {
                            const emailSubject = lang === 'ar' ? title : notification.title;
                            const emailMessage = lang === 'ar' ? message : notification.message;
                            
                            await sendMail(
                                user.email, 
                                emailSubject, 
                                emailMessage, 
                                lang, 
                                { name: user.fname || 'Customer' }
                            );
                        } catch (emailError) {
                            console.error(`Email notification error for notification ${notification.id}:`, emailError);
                        }
                    }

                    recordAuditLog(AuditLogAction.NOTIFICATION_SENT, {
                        userId: reqDetails.actorUserId || user.id,
                        entityName: 'Notification',
                        entityId: notification.id.toString(),
                        newValues: notification,
                        description: `Notification sent to user ${user.email}.`,
                        ipAddress: reqDetails.ipAddress,
                        userAgent: reqDetails.userAgent,
                    });

                } catch (bgError) {
                    console.error(`Background task error for notification ${notification.id}:`, bgError);
                }
            });

            return immediateResponse;
        } catch (error) {
            console.error(`Failed to create notification: ${error.message}`);
            throw new Error(`Failed to create notification: ${error.message}`);
        }
    },

    // 5. Mark Notifications as Read
    async markNotificationsAsRead(notificationIds, userUid, lang = 'en', reqDetails = {}) {
        try {
            // Find user by UID
            const user = await prisma.user.findUnique({ where: { uid: userUid } });
            if (!user) throw new Error('User not found');

            // Ensure notificationIds is an array
            const idsArray = Array.isArray(notificationIds) ? notificationIds : [notificationIds];
            const ids = idsArray.map(id => parseInt(id));

            // Update notifications to mark as read
            const updatedNotifications = await prisma.notification.updateMany({
                where: {
                    id: { in: ids },
                    userId: user.id // Ensure user can only mark their own notifications as read
                },
                data: {
                    isRead: true,
                    updatedAt: new Date()
                }
            });

            const immediateResponse = lang === 'ar' ? {
                message: 'تم تحديث حالة الإشعارات بنجاح.',
                updatedCount: updatedNotifications.count
            } : {
                message: 'Notifications marked as read successfully.',
                updatedCount: updatedNotifications.count
            };

            setImmediate(async () => {
                try {
                    // Clear relevant caches
                    if (redisClient.isReady) {
                        const keysToDel = [
                            cacheKeys.userNotificationsAr(user.uid)
                        ];
                        
                        // Clear individual notification caches for the marked notifications
                        ids.forEach(id => {
                            keysToDel.push(cacheKeys.notificationAr(id));
                        });
                        
                        const allNotificationsKeys = await redisClient.keys(cacheKeys.allNotificationsAr('*'));
                        if (allNotificationsKeys.length) keysToDel.push(...allNotificationsKeys);
                        
                        if (keysToDel.length > 0) await redisClient.del(keysToDel);

                        // Update cached user notifications - get from cache, update specific notifications, and cache back
                        if (deeplClient) {
                            const userNotificationsCacheKey = cacheKeys.userNotificationsAr(user.uid);
                            const cachedUserNotifications = await redisClient.get(userNotificationsCacheKey);
                            
                            if (cachedUserNotifications) {
                                // Parse cached notifications and update specific ones
                                const parsedNotifications = JSON.parse(cachedUserNotifications);
                                const updatedCachedNotifications = parsedNotifications.map(notification => {
                                    if (ids.includes(notification.id)) {
                                        return { ...notification, isRead: true, updatedAt: new Date() };
                                    }
                                    return notification;
                                });
                                
                                // Cache the updated user notifications
                                await redisClient.setEx(
                                    userNotificationsCacheKey, 
                                    AR_CACHE_EXPIRATION, 
                                    JSON.stringify(updatedCachedNotifications)
                                );
                            }

                            // Get the updated notifications from database and cache individually
                            const updatedNotificationsList = await prisma.notification.findMany({
                                where: { id: { in: ids } },
                                include: { 
                                    user: { select: { uid: true, fname: true, lname: true, email: true } }
                                }
                            });

                            // Translate and cache individual updated notifications
                            for (const notification of updatedNotificationsList) {
                                const translatedNotification = await translateNotificationFields(notification, 'AR', 'EN');
                                await redisClient.setEx(
                                    cacheKeys.notificationAr(notification.id), 
                                    AR_CACHE_EXPIRATION, 
                                    JSON.stringify(translatedNotification)
                                );
                            }
                        }
                    }

                    recordAuditLog(AuditLogAction.NOTIFICATIONS_MARKED_AS_READ, {
                        userId: user.id,
                        entityName: 'Notification',
                        entityId: ids.join(','),
                        description: `${updatedNotifications.count} notifications marked as read by user ${user.email}.`,
                        ipAddress: reqDetails.ipAddress,
                        userAgent: reqDetails.userAgent,
                    });

                } catch (bgError) {
                    console.error(`Background task error for marking notifications as read:`, bgError);
                }
            });

            return immediateResponse;
        } catch (error) {
            console.error(`Failed to mark notifications as read: ${error.message}`);
            throw new Error(`Failed to mark notifications as read: ${error.message}`);
        }
    },

    // 6. Delete Notification
    async deleteNotification(id, lang = 'en', reqDetails = {}) {
        try {
            const notificationId = parseInt(id);
            const notificationToDelete = await prisma.notification.findUnique({ 
                where: { id: notificationId },
                include: { 
                    user: { select: { uid: true, fname: true, lname: true, email: true } }
                }
            });
            
            if (!notificationToDelete) throw new Error('Notification not found');

            const deletedNotification = await prisma.notification.delete({ 
                where: { id: notificationId } 
            });

            const immediateResponse = lang === 'ar' ? {
                message: 'تم حذف الإشعار بنجاح.',
                deletedNotificationId: deletedNotification.id
            } : {
                message: `Notification ${notificationId} deleted successfully.`,
                deletedNotificationId: deletedNotification.id
            };

            setImmediate(async () => {
                try {
                    // Clear relevant caches
                    if (redisClient.isReady) {
                        const keysToDel = [
                            cacheKeys.notificationAr(notificationId),
                            cacheKeys.userNotificationsAr(notificationToDelete.user.uid)
                        ];
                        
                        const allNotificationsKeys = await redisClient.keys(cacheKeys.allNotificationsAr('*'));
                        if (allNotificationsKeys.length) keysToDel.push(...allNotificationsKeys);
                        
                        if (keysToDel.length > 0) await redisClient.del(keysToDel);

                        // Update user notifications cache after deletion
                        if (deeplClient) {
                            const userNotifications = await prisma.notification.findMany({
                                where: { userId: notificationToDelete.userId },
                                include: { 
                                    user: { select: { uid: true, fname: true, lname: true, email: true } }
                                },
                                orderBy: { createdAt: 'desc' }
                            });

                            const translatedUserNotifications = await Promise.all(
                                userNotifications.map(n => translateNotificationFields(n, 'AR', 'EN'))
                            );
                            
                            await redisClient.setEx(
                                cacheKeys.userNotificationsAr(notificationToDelete.user.uid), 
                                AR_CACHE_EXPIRATION, 
                                JSON.stringify(translatedUserNotifications)
                            );
                        }
                    }

                    recordAuditLog(AuditLogAction.GENERAL_DELETE, {
                        userId: reqDetails.actorUserId,
                        entityName: 'Notification',
                        entityId: notificationId.toString(),
                        oldValues: notificationToDelete,
                        description: `Notification ${notificationId} deleted.`,
                        ipAddress: reqDetails.ipAddress,
                        userAgent: reqDetails.userAgent,
                    });

                } catch (bgError) {
                    console.error(`Background task error for notification deletion ${notificationId}:`, bgError);
                }
            });

            return immediateResponse;
        } catch (error) {
            console.error(`Failed to delete notification ${id}: ${error.message}`);
            throw new Error(`Failed to delete notification ${id}: ${error.message}`);
        }
    },

    // 7. Get Unread Notification Count by User UID
    async getUnreadNotificationCount(uid, lang = 'en') {
        try {
            const user = await prisma.user.findUnique({ where: { uid: uid } });
            if (!user) throw new Error('User not found');

            const unreadCount = await prisma.notification.count({
                where: {
                    userId: user.id,
                    isRead: false
                }
            });

            return {
                unreadCount,
                message: lang === 'ar' ? 
                    `لديك ${unreadCount} إشعار غير مقروء` : 
                    `You have ${unreadCount} unread notifications`
            };
        } catch (error) {
            console.error(`Failed to get unread notification count for user ${uid}: ${error.message}`);
            throw new Error(`Failed to get unread notification count for user ${uid}: ${error.message}`);
        }
    }
};

export default notificationService;
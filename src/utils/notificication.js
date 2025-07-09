import cron from 'node-cron';
import prisma from '../utils/prismaClient.js';
import { createClient } from "redis";
import * as deepl from "deepl-node";

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

// --- Redis Configuration ---
const REDIS_URL = process.env.REDIS_URL || "redis://default:YOUR_REDIS_PASSWORD@YOUR_REDIS_HOST:PORT";
const AR_CACHE_EXPIRATION = 86400; // 24 hours in seconds

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

// --- Cache Keys ---
// MODIFIED: Removed `allNotificationsAr` as it is not used for storage here.
const cacheKeys = {
    notificationAr: (notificationId) => `notification:${notificationId}:ar`,
    userNotificationsAr: (uid) => `user:${uid}:notifications:ar`,
    translationQueue: 'translation:queue',
    userTranslationQueue: 'user:translation:queue',
    queueProcessing: 'translation:processing',
    userQueueProcessing: 'user:translation:processing'
};

// --- Translation Queue System ---
class TranslationQueue {
    constructor() {
        this.isProcessing = false;
        this.isUserProcessing = false;
        this.translationCache = new Map();
    }

    // Add notification to translation queue
    async addToQueue(notificationId) {
        try {
            // Check if already in queue
            const queueItems = await redisClient.lRange(cacheKeys.translationQueue, 0, -1);
            const existsInQueue = queueItems.some(item => JSON.parse(item).notificationId === notificationId);

            if (existsInQueue) {
                console.log(`Notification ${notificationId} already in queue`);
                return;
            }

            // Check if already translated
            const existingTranslation = await redisClient.get(cacheKeys.notificationAr(notificationId));
            if (existingTranslation) {
                console.log(`Notification ${notificationId} already translated`);
                return;
            }

            const queueItem = {
                notificationId,
                addedAt: new Date().toISOString(),
                attempts: 0
            };

            await redisClient.rPush(cacheKeys.translationQueue, JSON.stringify(queueItem));
            console.log(`Added notification ${notificationId} to translation queue`);

            if (!this.isProcessing) {
                this.startProcessing();
            }
        } catch (error) {
            console.error(`Error adding notification ${notificationId} to queue:`, error.message);
        }
    }

    // Add user notifications to translation queue
    async addUserNotificationsToQueue(uid, missingNotificationIds) {
        try {
            const queueItem = {
                uid,
                notificationIds: missingNotificationIds,
                addedAt: new Date().toISOString(),
                attempts: 0
            };

            await redisClient.rPush(cacheKeys.userTranslationQueue, JSON.stringify(queueItem));
            console.log(`Added user ${uid} notifications to translation queue (${missingNotificationIds.length} notifications)`);

            if (!this.isUserProcessing) {
                this.startUserProcessing();
            }
        } catch (error) {
            console.error(`Error adding user ${uid} notifications to queue:`, error.message);
        }
    }

    // Compare and sync user notifications
    async compareAndSyncUserNotifications(uid) {
        try {
            console.log(`Comparing notifications for user ${uid}`);

            const existingCache = await redisClient.get(cacheKeys.userNotificationsAr(uid));
            const cachedNotifications = existingCache ? JSON.parse(existingCache) : [];

            const dbNotifications = await prisma.notification.findMany({
                where: { userId: uid },
                select: { id: true },
                orderBy: { id: 'asc' }
            });

            console.log(`DB found ${dbNotifications.length} notifications for user ${uid}`);
            console.log(`Cache found ${cachedNotifications.length} notifications for user ${uid}`);

            const cachedNotificationIds = cachedNotifications.map(n => n.id);
            const dbNotificationIds = dbNotifications.map(n => n.id);
            const missingNotificationIds = dbNotificationIds.filter(id => !cachedNotificationIds.includes(id));

            if (missingNotificationIds.length > 0) {
                console.log(`Found ${missingNotificationIds.length} notifications to translate for user ${uid}`);
                await this.addUserNotificationsToQueue(uid, missingNotificationIds);
            } else {
                console.log(`All notifications for user ${uid} are already cached`);
            }

            return {
                total: dbNotifications.length,
                cached: cachedNotifications.length,
                missing: missingNotificationIds.length,
                missingIds: missingNotificationIds
            };

        } catch (error) {
            console.error(`Error comparing notifications for user ${uid}:`, error.message);
            return { total: 0, cached: 0, missing: 0, missingIds: [] };
        }
    }

    // Process user notification translation queue
    async startUserProcessing() {
        if (this.isUserProcessing) {
            console.log('User queue processing already running');
            return;
        }

        this.isUserProcessing = true;
        await redisClient.set(cacheKeys.userQueueProcessing, 'true', { EX: 300 }); // 5 min lock
        console.log('Started user notification translation queue processing');

        try {
            while (true) {
                const queueItem = await redisClient.lPop(cacheKeys.userTranslationQueue);
                if (!queueItem) {
                    console.log('User translation queue empty, stopping processor');
                    break;
                }

                const item = JSON.parse(queueItem);
                console.log(`Processing user ${item.uid} notifications from queue`);

                try {
                    await this.processUserNotificationTranslation(item);
                    console.log(`Successfully processed user ${item.uid} notifications`);
                } catch (error) {
                    console.error(`Failed to process user ${item.uid} notifications:`, error.message);
                    
                    item.attempts++;
                    if (item.attempts < 3) {
                        await redisClient.rPush(cacheKeys.userTranslationQueue, JSON.stringify(item));
                        console.log(`Re-queued user ${item.uid} notifications (attempt ${item.attempts})`);
                    } else {
                        console.error(`Max attempts reached for user ${item.uid} notifications, discarding`);
                    }
                }

                console.log('Waiting 10 seconds before next user processing...');
                await this.delay(10000);
            }
        } catch (error) {
            console.error('User queue processing error:', error.message);
        } finally {
            this.isUserProcessing = false;
            await redisClient.del(cacheKeys.userQueueProcessing);
            console.log('User notification translation queue processing stopped');
        }
    }

    // Process user notification translation
    async processUserNotificationTranslation(queueItem) {
        const { uid, notificationIds } = queueItem;

        try {
            const existingCache = await redisClient.get(cacheKeys.userNotificationsAr(uid));
            let cachedNotifications = existingCache ? JSON.parse(existingCache) : [];

            const missingNotifications = await prisma.notification.findMany({
                where: { 
                    id: { in: notificationIds },
                    userId: uid
                },
                include: {
                    user: { select: { uid: true, fname: true, lname: true, email: true } }
                }
            });

            for (const notification of missingNotifications) {
                const translatedNotification = await this.translateNotificationFields(notification, 'AR', 'EN');
                
                // --- MODIFICATION START ---
                // 1. Cache the individually translated notification
                const individualCacheKey = cacheKeys.notificationAr(notification.id);
                await redisClient.setEx(individualCacheKey, AR_CACHE_EXPIRATION, JSON.stringify(translatedNotification));
                console.log(`Cached individual translated notification ${notification.id}`);
                // --- MODIFICATION END ---
                
                cachedNotifications.push({
                    ...translatedNotification,
                    isRead: notification.isRead || false,
                    readAt: notification.readAt || null,
                    createdAt: notification.createdAt
                });
            }

            cachedNotifications.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

            // 2. Update the user's complete notifications cache
            const userCacheKey = cacheKeys.userNotificationsAr(uid);
            await redisClient.setEx(userCacheKey, AR_CACHE_EXPIRATION, JSON.stringify(cachedNotifications));
            
            console.log(`Updated user ${uid} notifications cache with ${cachedNotifications.length} total notifications`);

        } catch (error) {
            console.error(`Error processing user ${uid} notification translation:`, error.message);
            throw error;
        }
    }

    // Process queue one by one
    async startProcessing() {
        if (this.isProcessing) {
            console.log('Queue processing already running');
            return;
        }

        this.isProcessing = true;
        await redisClient.set(cacheKeys.queueProcessing, 'true', { EX: 300 }); // 5 min lock
        console.log('Started translation queue processing');

        try {
            while (true) {
                const queueItem = await redisClient.lPop(cacheKeys.translationQueue);
                if (!queueItem) {
                    console.log('Translation queue empty, stopping processor');
                    break;
                }

                const item = JSON.parse(queueItem);
                console.log(`Processing notification ${item.notificationId} from queue`);

                try {
                    await this.processNotificationTranslation(item);
                    console.log(`Successfully processed notification ${item.notificationId}`);
                } catch (error) {
                    console.error(`Failed to process notification ${item.notificationId}:`, error.message);
                    
                    item.attempts++;
                    if (item.attempts < 3) {
                        await redisClient.rPush(cacheKeys.translationQueue, JSON.stringify(item));
                        console.log(`Re-queued notification ${item.notificationId} (attempt ${item.attempts})`);
                    } else {
                        console.error(`Max attempts reached for notification ${item.notificationId}, discarding`);
                    }
                }

                console.log('Waiting 10 seconds before next translation...');
                await this.delay(10000);
            }
        } catch (error) {
            console.error('Queue processing error:', error.message);
        } finally {
            this.isProcessing = false;
            await redisClient.del(cacheKeys.queueProcessing);
            console.log('Translation queue processing stopped');
        }
    }

   // Process individual notification translation
async processNotificationTranslation(queueItem) {
    const { notificationId } = queueItem;

    const notification = await prisma.notification.findUnique({
        where: { id: notificationId },
        include: {
            user: { select: { uid: true, fname: true, lname: true, email: true } }
        }
    });

    if (!notification || !notification.user) {
        console.error(`Notification ${notificationId} not found or has no user`);
        return;
    }

    // Translate the notification
    const translatedNotification = await this.translateNotificationFields(notification, 'AR', 'EN');

    // 1. Cache the individual translated notification (existing logic)
    const individualCacheKey = cacheKeys.notificationAr(notificationId);
    await redisClient.setEx(individualCacheKey, AR_CACHE_EXPIRATION, JSON.stringify(translatedNotification));
    console.log(`Cached translated notification ${notificationId}`);

    // --- MODIFICATION START ---
    // 2. Also, update the user's full notification list cache

    try {
        const userUid = notification.user.uid;
        const userCacheKey = cacheKeys.userNotificationsAr(userUid);

        // Get the user's current cached list
        const existingUserCache = await redisClient.get(userCacheKey);
        let userNotifications = existingUserCache ? JSON.parse(existingUserCache) : [];

        // Add the new notification if it's not already in the list
        const isAlreadyInList = userNotifications.some(n => n.id === translatedNotification.id);
        if (!isAlreadyInList) {
            userNotifications.push({
                ...translatedNotification,
                isRead: notification.isRead || false,
                readAt: notification.readAt || null,
                createdAt: notification.createdAt
            });

            // Sort the list by date to keep it consistent
            userNotifications.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

            // Save the updated full list back to the user's cache
            await redisClient.setEx(userCacheKey, AR_CACHE_EXPIRATION, JSON.stringify(userNotifications));
            console.log(`Updated user ${userUid}'s notification list for notification ${notificationId}`);
        } else {
            console.log(`Notification ${notificationId} was already in user ${userUid}'s cached list.`);
        }
    } catch (error) {
        console.error(`Failed to update user's full cache for notification ${notificationId}:`, error.message);
    }
    // --- MODIFICATION END ---
}

    // Check for new untranslated notifications and add to queue
    async checkAndAddNewNotifications() {
        try {
            console.log('Checking for new untranslated notifications...');

            const allNotificationIds = await prisma.notification.findMany({
                select: { id: true },
                orderBy: { id: 'asc' }
            });

            const cachedKeys = await redisClient.keys('notification:*:ar');
            const cachedNotificationIds = cachedKeys.map(key => {
                const match = key.match(/notification:(\d+):ar/);
                return match ? parseInt(match[1]) : null;
            }).filter(id => id !== null);

            const queueItems = await redisClient.lRange(cacheKeys.translationQueue, 0, -1);
            const queuedNotificationIds = queueItems.map(item => JSON.parse(item).notificationId);

            const untranslatedIds = allNotificationIds
                .map(n => n.id)
                .filter(id => !cachedNotificationIds.includes(id) && !queuedNotificationIds.includes(id));

            console.log(`Found ${untranslatedIds.length} new notifications to translate`);

            for (const notificationId of untranslatedIds) {
                await this.addToQueue(notificationId);
            }

            if (untranslatedIds.length > 0 && !this.isProcessing) {
                this.startProcessing();
            }

        } catch (error) {
            console.error('Error checking for new notifications:', error.message);
        }
    }
    
    // Check for users with missing notification translations
    async checkAndSyncUserNotifications() {
        try {
            const usersWithNotifications = await prisma.notification.findMany({
                select: { userId: true },
                distinct: ['userId']
            });

            console.log(`Found ${usersWithNotifications.length} users with notifications to check`);

            for (const user of usersWithNotifications) {
                await this.compareAndSyncUserNotifications(user.userId);
                await this.delay(1000); // Small delay between users
            }

        } catch (error) {
            console.error('Error checking user notifications:', error.message);
        }
    }

    // Get queue status
    async getQueueStatus() {
        try {
            const queueLength = await redisClient.lLen(cacheKeys.translationQueue);
            const userQueueLength = await redisClient.lLen(cacheKeys.userTranslationQueue);
            const isProcessing = await redisClient.get(cacheKeys.queueProcessing);
            const isUserProcessing = await redisClient.get(cacheKeys.userQueueProcessing);
            
            return {
                notificationQueue: {
                    queueLength,
                    isProcessing: isProcessing === 'true',
                    currentlyProcessing: this.isProcessing
                },
                userQueue: {
                    queueLength: userQueueLength,
                    isProcessing: isUserProcessing === 'true',
                    currentlyProcessing: this.isUserProcessing
                }
            };
        } catch (error) {
            console.error('Error getting queue status:', error.message);
            return { 
                notificationQueue: { queueLength: 0, isProcessing: false, currentlyProcessing: false },
                userQueue: { queueLength: 0, isProcessing: false, currentlyProcessing: false }
            };
        }
    }

    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async translateText(text, targetLang, sourceLang = null) {
        let currentClient = getActiveDeepLClient();
        
        if (!currentClient) {
            console.warn("DeepL client is not initialized.");
            return text;
        }

        if (!text || typeof text !== 'string') {
            return text;
        }

        const cacheKey = `${text}::${sourceLang || 'auto'}::${targetLang}`;
        if (this.translationCache.has(cacheKey)) {
            return this.translationCache.get(cacheKey);
        }

        try {
            let retries = 3;
            let keysSwitched = 0;
            
            while (retries > 0 && keysSwitched < deeplKeys.length) {
                try {
                    const result = await currentClient.translateText(text, sourceLang, targetLang);
                    console.log(`Translated: "${text}" => "${result.text}"`);
                    this.translationCache.set(cacheKey, result.text);
                    return result.text;
                } catch (err) {
                    if (err.message.includes("Too many requests") || err.message.includes("quota")) {
                        console.warn(`DeepL rate limit/quota hit with key ${currentKeyIndex + 1}, switching key...`);
                        switchToNextKey();
                        currentClient = getActiveDeepLClient();
                        keysSwitched++;
                        await this.delay(2000);
                        retries--;
                    } else {
                        throw err;
                    }
                }
            }
            throw new Error("Failed after trying all available DeepL keys.");

        } catch (error) {
            console.error(`DeepL Translation error: ${error.message}`);
            return text;
        }
    }

    async translateNotificationFields(notification, targetLang, sourceLang = null) {
        if (!notification) return notification;
        
        const translatedNotification = { ...notification };
        
        if (notification.title) {
            translatedNotification.title = await this.translateText(notification.title, targetLang, sourceLang);
        }
        if (notification.message) {
            translatedNotification.message = await this.translateText(notification.message, targetLang, sourceLang);
        }
        if (notification.type) {
            translatedNotification.type = await this.translateText(notification.type, targetLang, sourceLang);
        }
        if (notification.entityType) {
            translatedNotification.entityType = await this.translateText(notification.entityType, targetLang, sourceLang);
        }
        
        if (notification.user) {
            translatedNotification.user = {
                ...notification.user,
                fname: await this.translateText(notification.user.fname, targetLang, sourceLang),
                lname: await this.translateText(notification.user.lname, targetLang, sourceLang)
            };
        }
        
        return translatedNotification;
    }
}

// --- Auto Translation Scheduler ---
class NotificationTranslationScheduler {
    constructor() {
        this.queue = new TranslationQueue();
        this.lastRunTime = null;
        this.lastUserSyncTime = null;
    }

    async syncArabicNotifications() {
        try {
            console.log('Starting notification translation sync...');
            this.lastRunTime = new Date();
            
            await this.queue.checkAndAddNewNotifications();
            await this.syncUserNotifications();
            
        } catch (error) {
            console.error('Translation sync error:', error.message);
        }
    }

    async syncUserNotifications() {
        try {
            console.log('Starting user notification translation sync...');
            this.lastUserSyncTime = new Date();
            await this.queue.checkAndSyncUserNotifications();
        } catch (error) {
            console.error('User notification sync error:', error.message);
        }
    }

    async addNotificationToQueue(notificationId) {
        await this.queue.addToQueue(notificationId);
    }

    async forceRefreshAllCaches() {
        try {
            console.log('Force refreshing all notification caches...');
            
            // --- MODIFICATION START ---
            // Simplified to only clear the relevant keys.
            const allArKeys = await redisClient.keys('notification:*:ar');
            const allUserArKeys = await redisClient.keys('user:*:notifications:ar');
            const allKeys = [...allArKeys, ...allUserArKeys];
            // --- MODIFICATION END ---
            
            if (allKeys.length > 0) {
                await redisClient.del(allKeys);
                console.log(`Cleared ${allKeys.length} cached entries`);
            }
            
            await redisClient.del(cacheKeys.translationQueue);
            await redisClient.del(cacheKeys.userTranslationQueue);
            
            await this.syncArabicNotifications();
            
        } catch (error) {
            console.error('Force refresh error:', error.message);
        }
    }

    async getSyncStatus() {
        const queueStatus = await this.queue.getQueueStatus();
        return {
            lastRunTime: this.lastRunTime,
            lastUserSyncTime: this.lastUserSyncTime,
            nextScheduledRun: this.getNextRunTime(),
            queues: queueStatus
        };
    }

    getNextRunTime() {
        const now = new Date();
        const nextRun = new Date(now);
        nextRun.setMinutes(nextRun.getMinutes() + 1);
        nextRun.setSeconds(0, 0);
        return nextRun;
    }

    start() {
        cron.schedule('*/1 * * * *', async () => {
            console.log('Scheduled notification translation sync started');
            await this.syncArabicNotifications();
        }, {
            timezone: "UTC"
        });

        setTimeout(async () => {
            console.log('Initial notification translation sync started');
            await this.syncArabicNotifications();
        }, 30000);

        console.log('Notification translation scheduler configured');
        console.log('Schedule: Every 1 minute');
    }

    stop() {
        cron.getTasks().forEach(task => task.stop());
        console.log('Notification translation scheduler stopped');
    }
}

const translationScheduler = new NotificationTranslationScheduler();

export default translationScheduler;
import notificationService from '../services/notificationService.js';

const notificationController = {

    // GET /api/notifications - Get all notifications with filters
    async getAllNotifications(req, res) {
        try {
            const lang = req.query.lang || req.headers['accept-language'] || 'en';
            const { ...filters } = req.query;
            const result = await notificationService.getAllNotifications(filters, lang);
            
            res.status(200).json({
                success: true,
                data: result,
                message: lang === 'ar' ? 'تم جلب الإشعارات بنجاح' : 'Notifications retrieved successfully'
            });
        } catch (error) {
            res.status(500).json({
                success: false,
                message: error.message,
                error: error.message
            });
        }
    },

    // GET /api/notifications/user/:uid - Get notifications by user UID
    async getNotificationsByUserUid(req, res) {
        try {
            const uid = req.user.uid;
            console.log('User UID:', uid);
            const { ...filters } = req.query;
            const lang = req.query.lang || req.headers['accept-language'] || 'en';

            const result = await notificationService.getNotificationsByUserUid(uid, filters, lang);
            
            res.status(200).json({
                success: true,
                data: result,
                message: lang === 'ar' ? 'تم جلب إشعارات المستخدم بنجاح' : 'User notifications retrieved successfully'
            });
        } catch (error) {
            res.status(404).json({
                success: false,
                message: error.message,
                error: error.message
            });
        }
    },

    // GET /api/notifications/:id - Get notification by ID
    async getNotificationById(req, res) {
        try {
            const { id } = req.params;
            const { lang = 'en' } = req.query;
            
            const notification = await notificationService.getNotificationById(id, lang);
            
            if (!notification) {
                return res.status(404).json({
                    success: false,
                    message: lang === 'ar' ? 'الإشعار غير موجود' : 'Notification not found'
                });
            }
            
            res.status(200).json({
                success: true,
                data: notification,
                message: lang === 'ar' ? 'تم جلب الإشعار بنجاح' : 'Notification retrieved successfully'
            });
        } catch (error) {
            res.status(500).json({
                success: false,
                message: error.message,
                error: error.message
            });
        }
    },

    // POST /api/notifications - Create notification
    async createNotification(req, res) {
        try {
            const { lang = 'en' } = req.query;
            const { userId, title, message, type, link, entityId, entityType } = req.body;
            
            // Validation
            if (!userId || !title || !message) {
                return res.status(400).json({
                    success: false,
                    message: lang === 'ar' ? 
                        'المعرف الخاص بالمستخدم والعنوان والرسالة مطلوبة' : 
                        'User ID, title, and message are required'
                });
            }

            const reqDetails = {
                actorUserId: req.user?.id,
                ipAddress: req.ip || req.connection.remoteAddress,
                userAgent: req.get('User-Agent')
            };

            const result = await notificationService.createNotification(
                { userId, title, message, type, link, entityId, entityType },
                lang,
                reqDetails
            );
            
            res.status(201).json({
                success: true,
                data: result,
                message: result.message
            });
        } catch (error) {
            res.status(500).json({
                success: false,
                message: error.message,
                error: error.message
            });
        }
    },

    // PATCH /api/notifications/mark-read - Mark notifications as read
    async markNotificationsAsRead(req, res) {
        try {
            const lang = req.query.lang || req.headers['accept-language'] || 'en';
            const { notificationIds } = req.body;
            
            // Ensure notificationIds is an array
            const idsArray = Array.isArray(notificationIds) ? notificationIds : [notificationIds];
            const userUid = req.user?.uid;
            
            if (!notificationIds || !userUid) {
                return res.status(400).json({
                    success: false,
                    message: lang === 'ar' ? 
                        'معرفات الإشعارات ومعرف المستخدم مطلوبة' : 
                        'Notification IDs and user UID are required'
                });
            }

            const reqDetails = {
                actorUserId: req.user?.id,
                ipAddress: req.ip || req.connection.remoteAddress,
                userAgent: req.get('User-Agent')
            };

            const result = await notificationService.markNotificationsAsRead(
                notificationIds,
                userUid,
                lang,
                reqDetails
            );
            
            res.status(200).json({
                success: true,
                data: result,
                message: result.message
            });
        } catch (error) {
            res.status(500).json({
                success: false,
                message: error.message,
                error: error.message
            });
        }
    },

    // DELETE /api/notifications/:id - Delete notification
    async deleteNotification(req, res) {
        try {
            const { id } = req.params;
            const lang = req.query.lang || req.headers['accept-language'] || 'en';

            const reqDetails = {
                actorUserId: req.user?.id,
                ipAddress: req.ip || req.connection.remoteAddress,
                userAgent: req.get('User-Agent')
            };

            const result = await notificationService.deleteNotification(id, lang, reqDetails);

            if (!result) {
                return res.status(404).json({
                    success: false,
                    message: lang === 'ar' ? 'الإشعار غير موجود' : 'Notification not found'
                });
            }

            res.status(200).json({
                success: true,
                data: result,
                message: result.message
            });
        } catch (error) {
            const statusCode = error.message.includes('not found') ? 404 : 500;
            res.status(statusCode).json({
                success: false,
                message: error.message,
                error: error.message
            });
        }
    },

    // GET /api/notifications/user/:uid/unread-count - Get unread notification count
    async getUnreadNotificationCount(req, res) {
        try {
            const uid = req.user?.uid;
            const lang = req.query.lang || req.headers['accept-language'] || 'en';
            
            const result = await notificationService.getUnreadNotificationCount(uid, lang);
            
            res.status(200).json({
                success: true,
                data: result,
                message: lang === 'ar' ? 'تم جلب عدد الإشعارات غير المقروءة بنجاح' : 'Unread notification count retrieved successfully'
            });
        } catch (error) {
            res.status(404).json({
                success: false,
                message: error.message,
                error: error.message
            });
        }
    }
};

export default notificationController;
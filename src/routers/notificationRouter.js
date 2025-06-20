import express from 'express';
import notificationController from '../controllers/notificationController.js';
import verifyToken from '../middlewares/verifyToken.js'; // Uncomment if you need to protect routes

const router = express.Router();

// GET /api/notifications - Get all notifications with filters
router.get('/', verifyToken, notificationController.getAllNotifications);

// GET /api/notifications/self - Get notifications by user UID
router.get('/self', verifyToken, notificationController.getNotificationsByUserUid);

// GET /api/notifications/self/unread-count - Get unread notification count
router.get('/self/unread-count', verifyToken, notificationController.getUnreadNotificationCount);

// GET /api/notifications/:id - Get notification by ID
router.get('/:id', verifyToken, notificationController.getNotificationById);

// POST /api/notifications - Create notification
router.post('/', verifyToken, notificationController.createNotification);

// PATCH /api/notifications/mark-read - Mark notifications as read
router.put('/mark-read', verifyToken, notificationController.markNotificationsAsRead);

// DELETE /api/notifications/:id - Delete notification
router.delete('/:id', verifyToken, notificationController.deleteNotification);

export default router;
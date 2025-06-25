import express from 'express';
import userController from '../controllers/userController.js';
import verifyToken from '../middlewares/verifyToken.js'; // Protect routes that need authentication

const router = express.Router();

router.post('/', userController.createUser); // Public: User registration
// router.post('/login', userController.loginUser); // Public: User login

// Routes below this could be protected
// router.use(verifyToken); // Apply verifyToken middleware to all subsequent routes in this router

router.get('/', verifyToken, userController.getAllUsers); // Example: Admin only or for specific purposes
router.get('/searchid/:id', verifyToken, userController.getUserById); // User can get their own, or admin can get any
router.get('/self', verifyToken, userController.getUserByUid); // User can get their own by UID, or admin can get any by UID
router.put('/update/:id', verifyToken, userController.updateUser); // User can update their own, or admin can update any
router.delete('/delete/:id', verifyToken, userController.deleteUser); // Admin action or user can delete their own account
router.post('/login', userController.loginUser); // Public: User login
router.post('/reset-password', userController.resetPassword); // Public: Password reset
router.get('/find-email/:email', userController.findUserByEmail); // Public: Find user by email

export default router;
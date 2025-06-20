import express from 'express';
import reviewController from '../controllers/reviewController.js';
import verifyToken from '../middlewares/verifyToken.js';

const router = express.Router();

// Create a new review (requires authentication)
router.post('/', verifyToken, reviewController.createReview);

// Get all reviews with filters (public route)
router.get('/', verifyToken,reviewController.getAllReviews);

// Get review by ID (public route)
router.get('/:id',verifyToken, reviewController.getReviewById);

// Get reviews by user UID (public route)
router.get('/user/review', verifyToken, reviewController.getReviewsByUserUid);

// Update review (requires authentication for users, optional for admin)
router.put('/:id', verifyToken, reviewController.updateReview);

// Delete review (requires authentication)
router.delete('/:id', verifyToken, reviewController.deleteReview);

export default router;
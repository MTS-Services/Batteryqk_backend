import express from 'express';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import cors from 'cors';

// Load environment variables
dotenv.config();

// Import Routers
import userRouter from './routers/userRouter.js';
import categoryRouter from './routers/categoryRouter.js';
import listingRouter from './routers/listingRouter.js';
import BookingRouter from './routers/bookingRouter.js'; // If you have a booking router, import it here
import reviewRouter from './routers/reviewRouter.js'; // If you have a review router, import it here
import notificationRouter from './routers/notificationRouter.js'; // If you have a notification router, import it here
import translationScheduler from './utils/notificication.js'; // Import translation scheduler


// Import Middlewares
import errorHandler from './middlewares/errorHandler.js';
import { UPLOAD_DIR } from './middlewares/multer.js'; // For serving static files
import { getLanguage } from './utils/i18n.js'; // For setting response language

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

// --- CRITICAL CONFIGURATION CHECKS ---
let criticalConfigMissing = false;

//allow all origins for CORS
app.use(cors({
  origin: '*', // Be cautious with '*' in production
  methods: ['GET', 'POST', 'PUT', 'DELETE','PATCH'],
  credentials: true,
}));

if (!process.env.DATABASE_URL) {
  console.error("FATAL ERROR: DATABASE_URL is not set in .env file!");
  criticalConfigMissing = true;
}
if (!process.env.SECRET_CODE) {
  console.error("FATAL ERROR: SECRET_CODE is not set in .env file! JWT functionality will be broken.");
  criticalConfigMissing = true;
}
// Add any other absolutely essential environment variable checks here
// For example, if email sending is critical for core features upon startup:
// if (!process.env.EMAIL_USER || !process.env.EMAIL_PASS) {
//   console.error("FATAL ERROR: EMAIL_USER or EMAIL_PASS not set. Critical email functions will fail.");
//   criticalConfigMissing = true;
// }

if (criticalConfigMissing) {
  console.error("Application cannot start due to missing critical configurations. Please check your .env file.");
  process.exit(1); // Exit the application with an error code
}
// --- END CRITICAL CONFIGURATION CHECKS ---


// Middlewares
app.use(express.json()); // For parsing application/json
app.use(express.urlencoded({ extended: true })); // For parsing application/x-www-form-urlencoded

// Middleware to set response language based on request
app.use((req, res, next) => {
  const lang = getLanguage(req);
  res.setHeader('Content-Language', lang);
  next();
});

// Static files (for uploaded images)
app.use('/uploads', express.static(UPLOAD_DIR));


// Routes
app.get('/', (req, res) => {
  res.send('API is running...');
});

app.use('/api/users', userRouter);
app.use('/api/categories', categoryRouter);
app.use('/api/listings', listingRouter);
app.use('/api/bookings', BookingRouter); // If you have a booking router, use it here
app.use('/api/reviews', reviewRouter); // If you have a review router, use it here
app.use('/api/notifications', notificationRouter); // If you have a notification router, use it here



// if other  mean which i not declare then say hi hackers
app.use((req, res) => {
 res.send('Hi Hackers, you are not allowed to access this API');
});




// --- Global Error Handling Middleware ---
// Must have 4 arguments for Express to recognize it as error handler
app.use((err, req, res, next) => {
    err.statusCode = err.statusCode || 500; // Default to 500 Internal Server Error
    err.status = err.status || 'error';

    console.error('ERROR 💥:', err); // Log the full error stack

    // Send response
    res.status(err.statusCode).json({
        status: err.status,
        message: err.message,
        // Optionally include stack trace in development
        // stack: process.env.NODE_ENV === 'development' ? err.stack : undefined,
        error: err // Include the error object itself can sometimes be useful (or strip it in prod)
    });
});



// Start the server only if all critical checks passed
app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);

  // Initialize translation scheduler
  translationScheduler.start();

  // You can keep non-critical warnings here
  if (!process.env.EMAIL_USER || !process.env.EMAIL_PASS) {
      // This check might be redundant if you made it critical above,
      // or you can have different levels of criticality.
      console.warn("WARNING: EMAIL_USER or EMAIL_PASS not set. Email sending will fail if not critical.");
  }
  // You might also want to establish and check your database connection here
  // and potentially exit if it fails, or implement a retry mechanism.
});

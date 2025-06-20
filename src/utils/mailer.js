import nodemailer from 'nodemailer';
import { translate } from './i18n.js';

// const transporter = nodemailer.createTransport({
//   // Configure your email transport (e.g., SMTP, SendGrid, etc.)
//   // Example for Gmail (requires "less secure app access" or OAuth2)
//   service: 'gmail',
//   auth: {
//     user: process.env.EMAIL_USER, // your_email@gmail.com
//     pass: process.env.EMAIL_PASS, // your_email_password or app password
//   },
// });


const transporter = nodemailer.createTransport({
  host: process.env.EMAIL_HOST, // smtp.hostinger.com
  port: parseInt(process.env.EMAIL_PORT), // 465
  secure: process.env.EMAIL_SECURE === 'true', // true for 465
  auth: {
    user: process.env.EMAIL_USER, 
    pass: process.env.EMAIL_PASS, 
  },
});


const sendMail = async (to, subjectKey, bodyKey, lang = 'en', templateData = {}) => {
  const subject = translate(subjectKey, lang, templateData);
  const text = translate(bodyKey, lang, templateData);
  const html = `<p>${text.replace(/\n/g, '<br>')}</p>`; // Simple HTML version

  try {
    await transporter.sendMail({
      from: `"Batteryqk" <${process.env.EMAIL_USER}>`,
      to,
      subject,
      text,
      html,
    });
    console.log(`Email sent to ${to} with subject "${subject}"`);
  } catch (error) {
    console.error('Error sending email:', error);
  }
};

export { sendMail };
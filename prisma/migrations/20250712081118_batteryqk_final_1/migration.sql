-- DropForeignKey
ALTER TABLE "Booking" DROP CONSTRAINT "Booking_listing_id_fkey";

-- DropForeignKey
ALTER TABLE "Reward" DROP CONSTRAINT "Reward_booking_id_fkey";

-- AddForeignKey
ALTER TABLE "Booking" ADD CONSTRAINT "Booking_listing_id_fkey" FOREIGN KEY ("listing_id") REFERENCES "Listing"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Reward" ADD CONSTRAINT "Reward_booking_id_fkey" FOREIGN KEY ("booking_id") REFERENCES "Booking"("id") ON DELETE CASCADE ON UPDATE CASCADE;

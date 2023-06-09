/*
  Warnings:

  - You are about to drop the column `table` on the `results` table. All the data in the column will be lost.

*/
-- AlterTable
ALTER TABLE `results` DROP COLUMN `table`;

-- CreateTable
CREATE TABLE `ComparedResults` (
    `id` VARCHAR(255) NOT NULL,
    `ticketListValue` VARCHAR(255) NOT NULL,
    `cdpSanitizedValue` VARCHAR(255) NOT NULL,

    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

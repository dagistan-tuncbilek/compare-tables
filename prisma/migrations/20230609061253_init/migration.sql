-- CreateTable
CREATE TABLE `Results` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `modelId` VARCHAR(255) NOT NULL,
    `value` VARCHAR(255) NOT NULL,
    `table` VARCHAR(255) NOT NULL,

    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

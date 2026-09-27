CREATE TABLE public.downloads (
    download_id  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id      UUID         NOT NULL,
    ip           VARCHAR(45)  NOT NULL,
    country      CHAR(2)      NOT NULL,
    tool         VARCHAR(32)  NOT NULL,
    version      VARCHAR(16)  NOT NULL,
    os           VARCHAR(16)  NOT NULL,
    channel      VARCHAR(16)  NOT NULL,
    status       VARCHAR(16)  NOT NULL,
    size_bytes   BIGINT       NOT NULL,
    duration_ms  INT,
    created_at   TIMESTAMP(3) NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP(3) NOT NULL DEFAULT now()
);

ALTER TABLE public.downloads REPLICA IDENTITY FULL;

CREATE PUBLICATION flink_pub FOR TABLE public.downloads;

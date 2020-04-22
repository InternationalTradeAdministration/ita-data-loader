create TABLE DATALOADER_STORAGE_LOG (
    [FILE_NAME] VARCHAR(510),
    [SNAPSHOT] VARCHAR(510),
    [URL] VARCHAR(510),
    [SIZE] BIGINT,
    [CONTAINER] VARCHAR(255),
    [UPLOADED_AT] DATETIME,
    [UPLOADED_BY] VARCHAR(255),
    [PII] BIT
);
go
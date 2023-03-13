TEXT

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--=================================================================================    
--change By         Changed ON           --Reason     
--Ravindra Sapkal   21-July-2022         Remove the cuid   
--Ravindra Sapkal   01-Aug-2022          Remove the Exists check. 
--=================================================================================    
CREATE PROCEDURE [dbo].[proc_StagingCKYCAttachmentDataLoad] @ProspectNo VARCHAR(20)
	,@filename VARCHAR(100)
	,@Path VARCHAR(1000)
	,@TransactionID INT
	,@AddedBy VARCHAR(20)
	,@CUID VARCHAR(20)
	,@doctype VARCHAR(100)
	,@binaryFormat VARCHAR(1)
	,@BatchId VARCHAR(100)
	,@Product VARCHAR(50)
AS
BEGIN
	DECLARE @Statement VARCHAR(100);

	IF (ISNULL(@ProspectNo, '') <> '')
	BEGIN
		----DELETE EXISTING record    
		DELETE
		FROM [StagingCKYCAttachmentView]
		WHERE SourceSysytemCustomerCode = @CUID
			AND AppRefnumberForImages = @ProspectNo
			AND ImageName = @filename
			AND Product = @Product
			AND AttachmentCode = @doctype

		INSERT INTO [dbo].[StagingCKYCAttachmentView] (
			TransactionId
			,ImageName
			,FilePath
			,FileExtension
			,AttachmentCode
			,AppRefnumberForImages
			,SourceSysytemCustomerCode
			,BinaryFormat
			,BatchId
			,Product
			,AddedBy
			,AddedOn
			)
		SELECT @TransactionID
			,REPLACE(@filename, '''', '')
			,LEFT(@Path, LEN(@Path) - 1)
			,CASE 
				WHEN @filename LIKE '%.%'
					THEN REVERSE(SUBSTRING(REVERSE(@filename), 1, CHARINDEX('.', REVERSE(@filename)) - 1))
				END
			,LOWER(@doctype)
			,@ProspectNo AS ProspectNo
			,@CUID
			,@binaryFormat
			,CAST(@BatchId AS INT)
			,@Product
			,@AddedBy
			,GETDATE() AS MkrDt;

		IF (@Product = 'PLBYJU')
		BEGIN
			UPDATE CustomerImagesDailyPLBYJU
			SET IsUploaded = 'Y'
				,ModifiedOn = GETDATE()
			WHERE Cuid = @CUID
				AND AppRefNoForImages = @ProspectNo
				AND DocumentType = @doctype
				AND FileName = REPLACE(@filename, '.jpg', '.png');;
		END
		ELSE IF (@Product = 'SME')
		BEGIN
			UPDATE CustomerImagesSME
			SET IsUploaded = 'Y'
				,ModifiedOn = GETDATE()
			WHERE Cuid = @CUID
				AND AppRefNoForImages = @ProspectNo
				AND documentType = @doctype
				AND FileName = REPLACE(@filename, '.jpg', '.png');
		END
		ELSE
		BEGIN
			UPDATE CustomerImagesGL
			SET IsUploaded = 'Y'
				,ModifiedOn = GETDATE()
			WHERE AppRefNoForImages = @ProspectNo
				AND documentType = @doctype
				AND FileName = REPLACE(@filename, '.jpg', '.png');
		END

		-------update success and failed count     
		DECLARE @RowSuccesscount INT = 0;
		DECLARE @RowFailedCount INT = 0;

		SELECT @RowSuccesscount = COUNT(1)
		FROM StagingCKYCAttachmentView WITH (NOLOCK)
		WHERE BatchId = @BatchId
			AND Product = @Product;

		SELECT @RowFailedCount = COUNT(1)
		FROM dbo.StagingCKYCAttachmentViewFailed WITH (NOLOCK)
		WHERE BatchId = @BatchId
			AND Product = @Product;

		EXEC InsertUpdateBatchDetails @Product
			,@BatchId
			,'sys'
			,@RowSuccesscount
			,@RowFailedCount
			,0;
			------------------------------------------------------    
	END;
	ELSE
	BEGIN
		SET @Statement = 'Cannot proceed further with blank prospect no.';
	END;
END;
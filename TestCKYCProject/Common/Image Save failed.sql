TEXT

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--=================================================================================    
--change By  : Ravindra Sapkal    
--Reason     : Added batch id    
--Changed On : 10 Jan 2019    
-------------------------------------------------------------------------------------------------    
CREATE PROCEDURE [dbo].[Proc_StagingCKYCAttachmentDataLoadFailed] @ProspectNo VARCHAR(12)
	,@filename VARCHAR(100)
	,@Path VARCHAR(1000)
	,@TransactionID INT
	,@AddedBy VARCHAR(20)
	,@CUID VARCHAR(20)
	,@doctype VARCHAR(100)
	,@BinaryFormat VARCHAR(1)
	,@BatchId VARCHAR(100)
	,@Product VARCHAR(50)
AS
BEGIN
	DECLARE @Statement VARCHAR(100)

	INSERT INTO [dbo].[StagingCKYCAttachmentViewFailed] (
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
		,replace(@filename, '''', '')
		,LEFT(@Path, LEN(@PATH) - 1)
		,CASE 
			WHEN @filename LIKE '%.%'
				THEN reverse(substring(reverse(@filename), 1, charindex('.', reverse(@filename)) - 1))
			END
		,@doctype
		,@ProspectNo AS ProspectNo
		,@CUID
		,@BinaryFormat
		,Cast(@BatchId AS INT)
		,@Product
		,@AddedBy
		,GETDATE() AS MkrDt
END
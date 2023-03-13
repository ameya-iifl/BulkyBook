--BEGIN TRANSACTION TEST

--ROLLBACK TRANSACTION
--CREATE PROC [dbo].[proc_CKYC_Customer217DataUploadGL] 
--@Product VARCHAR(50)
--,@BatchId INT


--AS
BEGIN


/*
Check again later
DECLARE @CurrentDate VARCHAR(8)= CONVERT (VARCHAR(8), GETDATE(), 112) 
DECLARE @RowsUploaded INT =0
DECLARE @RowsFailed INT=0
DECLARE @RowsFailedAtTSS INT=0

--------Generate Batch Id 
SET @batchId = 0



	IF 	@BatchId = 0 
		BEGIN

			DECLARE @BatchIdCal INT

			SELECT @BatchIdCal = NEXT VALUE FOR BatchSequenceNumber  

			INSERT INTO [dbo].[BatchDetails]([BatchId], [Product], [UploadDate], [RowsUploaded], [RowsFailed], [RowsFailedAtTSS], [MakerId])
			VALUES(	@BatchIdCal, -- BatchId - int
					@Product  , -- Product - varchar(50)
					GETDATE(), -- UploadDate - datetime
					0   , -- RowsUploaded - int
					0   , -- RowsFailed - int
					0   , -- RowsFailedAtTSS - int
					'SYS'  -- MakerId - varchar(10)
				)
		
			SELECT 0 Status , @BatchIdCal BatchId
		END 
		ELSE
		BEGIN
			UPDATE [dbo].[BatchDetails] 
			SET [RowsUploaded] = CASE WHEN @RowsUploaded <> 0 THEN @RowsUploaded ELSE RowsUploaded END,
				[RowsFailed] = CASE WHEN @RowsFailed <> 0 THEN @RowsFailed ELSE RowsFailed END,
				[RowsFailedAtTSS] = CASE WHEN @RowsFailedAtTSS <> 0 THEN @RowsFailedAtTSS ELSE RowsFailedAtTSS END
			WHERE [BatchId] = @BatchId AND [Product] = @Product
		END

*/

-------------------------------------------------------------------------------------------------------------------------------------------
--GoldLoan-----
--IF @Product ='GL'
BEGIN
		
		;WITH [CTE]
        AS (SELECT
		PA.ProspectNo
		,PA.CUID
		,PA.CreatedOn
		,PA.LoanStatus
		,'' AS CustomerStatusEffectiveDate
		,PA.Title
		,PA.FirstName
		,PA.MiddleName
		,PA.LastName
		,FI.FatherFirstName
		,FI.FatherMiddleName
		,FI.FatherLastName
		,FI.SpouseTitle
		,FI.SpouseFirstName
		,FI.SpouseMiddleName
		,FI.SpouseLastName
		,FI.MotherFirstName
		,FI.MotherMiddleName
		,FI.MotherLastName
		,PA.Gender
		,FI.MaritalStatus
		,AD.Country
		
		FROM ISOM.LOANS.PrimaryApplicant PA WITH (NOLOCK)
		JOIN ISOM.LOANS.FamilyInformation FI WITH (NOLOCK)
		ON PA.ProspectNo= FI.ProspectNo AND PA.BusinessCode = Fi.BusinessCode
		LEFT OUTER JOIN ISOM.LOANS.Addresses AD WITH (NOLOCK)
		ON PA.ProspectNo= AD.ProspectNo AND PA.BusinessCode = AD.BusinessCode
		WHERE PA.BusinessCode = 'GL'
		AND PA.ProspectNo IN
		   ('GL1575'
			,'GL11130'
			,'GL10399'
			,'GL2535'
			,'GL25355')
		)

	--INSERT INTO Clients.dbo.StagingCustom217CustomerFlat (
	--ApplicationRefNumber
	--,ParentCompany
	--,TransactionID
	--,SourceSystemName
	--,SourceSystemCustomerCode
	--,SourceSystemCustomerCreationDate
	--,IsSmallCustomer
	--,EkycOTPbased
	--,RecordIdentifier
	--,Segments
	--,SegmentStartDate
	--,ProductSegments
	--,CustomerStatus
	--,CustomerStatusEffectiveDate
	--,RelatedPartyStatus
	--,RelatedPartyStatusEffectiveDate
	--,CustomerType
	--,CustomerSubType
	--,Prefix
	--,FirstName
	--,MiddleName
	--,LastName
	--,MaidenPrefix
	--,MaidenFirstName
	--,MaidenMiddleName
	--,MaidenLastName
	--,FatherPrefix
	--,FatherFirstName
	--,FatherMiddleName
	--,FatherLastName
	--,SpousePrefix
	--,SpouseFirstName
	--,SpouseMiddleName
	--,SpouseLastName
	--,MotherPrefix
	--,MotherFirstName
	--,MotherMiddleName
	--,MotherLastName
	--,Gender
	--,MaritalStatus
	--,Citizenship
	--,CountryOfResidence
	--,OccupationType
	--,ActivitySector
	--,NatureOfBusiness
	--,NatureOfBusinessOther
	--,DateofBirth
		
	--)

	SELECT TOP 10
	ApplicationRefNumber
	,'IIFL12' AS ParentCompany
	,[FINAL].ID AS TransactionID
	,[FINAL].[SourceSystemName]
	,CUID [SourceSystemCustomerCode]
	,[SourceSystemCustomerCreationDate]
	,0 AS [IsSmallCustomer]
	,0 AS EkycOTPbased
	,'' AS RecordIdentifier
	,'' AS Segments
	,'' AS SegmentStartDate
	,'' AS ProductSegments
	,CASE WHEN CustomerStatus = 'ACT' THEN 'Active'
			    WHEN CustomerStatus = 'CLS' THEN 'Closed'
				ELSE 'Suspended' END AS CustomerStatus
	,CustomerStatusEffectiveDate
	,'' AS RelatedPartyStatus
	,'' AS RelatedPartyStatusEffectiveDate
	,[CustomerType]
	,'' CustomerSubType
	,LTRIM (RTRIM (REPLACE(ISNULL(Prefix,''),'.',''))) AS Prefix
	,[FirstName]
	,[MiddleName]
	,[LastName]
	,'' MaidenPrefix
	,'' MaidenFirstName
	,'' MaidenMiddleName
	,'' MaidenLastName
	,FatherPrefix
	,FatherFirstName
	,FatherMiddleName
	,FatherLastName
	,SpousePrefix
	,SpouseFirstName
	,SpouseMiddleName
	,SpouseLastName
	,MotherPrefix
	,MotherFirstName
	,MotherMiddleName
	,MotherLastName
	,Gender
	,MaritalStatus
	,Citizenship
	,CountryOfResidence
	--,[OccupationType]
	,'' AS ActivitySector
	,'' AS NatureOfBusiness
	,'' AS NatureOfBusinessOther

	FROM(
	SELECT 'GL' + CONVERT(VARCHAR(10), GETDATE(), 112) + CONVERT(VARCHAR(2), DATEPART(HOUR, GETDATE())) + CONVERT(VARCHAR(2), DATEPART(MINUTE, GETDATE()))+ RIGHT('0000000' + CONVERT(VARCHAR(10), DENSE_RANK() OVER (ORDER BY [CTE].[CUID])), 10) AS [ID]
	,'GoldLoan' AS [SourceSystemName]
	,CUID
	,CASE WHEN ISDATE([CTE].[CreatedOn]) = 1 THEN
                           FORMAT([CTE].[CreatedOn], 'dd-MMM-yyyy')
                       ELSE
                           NULL
                   END AS [SourceSystemCustomerCreationDate] 
	,'No' AS [SmallCustomer] --not necessary
	,'No' AS [EkycOTPbased] --not necessary
	,CTE.LoanStatus AS CustomerStatus
	,FORMAT(CONVERT(DATE, [CTE].CreatedOn), 'dd-MMM-yyyy') AS CustomerStatusEffectiveDate
	,'1' AS [CustomerType]
	,CTE.Title AS Prefix
	,CTE.FirstName
	,CTE.MiddleName
	,CTE.LastName
	,CASE WHEN ISNULL([CTE].[FatherFirstName], '') <> '' THEN
                           'Mr'
                       ELSE
                           ''
                   END AS [FatherPrefix]
	,CTE.FatherFirstName
	,CTE.FatherMiddleName
	,CTE.FatherLastName
	,CTE.SpouseTitle AS SpousePrefix
	,CTE.SpouseFirstName
	,CTE.SpouseMiddleName
	,CTE.SpouseLastName
	,CASE WHEN ISNULL([CTE].[MotherFirstName], '') <> '' THEN
                           'Mrs'
                       ELSE
                           ''
                   END AS [MotherPrefix]
	,CTE.MotherFirstName
	,CTE.MotherMiddleName
	,CTE.MotherLastName
	,CTE.Gender
	,CTE.MaritalStatus
	,'IN' Citizenship
	,CTE.Country AS [CountryOfResidence]
	,CTE.ProspectNo as [ApplicationRefNumber]

	FROM CTE
	) AS [FINAL] 

END
END
USE Clients
GO

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/*
Author		    Date		Remarks
------------------------------------------------------------
Shamita Das	    11-04-2019	Get the photograph, POI, POA of SME
Shamita Das	    05-03-2020	Changes as per ticket - INC-074653 corrected sales emp code to Sales Executive from sourcing page & removed hard-coding of 84,86 thus for all portfolios of SME
Ravindra Sapkal 28-09-2020  Changes as per TFS id for regulatory changes
Sameer Naik		12-07-2022	Add log for record count and update condition for Address proof
Sameer Naik		13-07-2022	Removed 3 images condition check 
Sameer Naik		14-07-2022	Commented IF permanent address is same as Correspondence Addresses
Sameer Naik		19-07-2022	Added error log.
Sameer Naik		05-08-2022	Update aadhar column in select list.
Ameya Mahale	15-12-2022	129763 - Removed dependency of smetab_documents 
*/

CREATE PROCEDURE dbo.proc_CKYC_Customer217DataUploadSME
AS
BEGIN

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

BEGIN TRY
    	DECLARE @BatchId INT = 0
	-----added batch logic for the populating the data customer images table
	CREATE TABLE #tempBatch (
		STATUS VARCHAR(10)
		,BatchId INT
		)

	INSERT INTO #tempBatch (
		[STATUS]
		,BatchId
		)
	EXECUTE InsertUpdateBatchDetails 'SME'
		,0
		,'SME_Job'
		,0
		,0
		,0

	SELECT @BatchId = BatchId
	FROM #tempBatch
	WHERE STATUS = 0
	------added batch logic 

	INSERT INTO dbo.CKYCDataUploadLog(processname,methodname,activity,CreatedOn,RecordsProcessed,BatchId,Response)
	VALUES('SME Ckyc Data Upload','proc_CKYC_Customer217DataUploadSME','Process-Start',GETDATE(),0,@BatchId,'Process-Start')

	IF OBJECT_ID('tempdb..#CustomerSME') IS NOT NULL
    DROP TABLE #CustomerSME

	IF OBJECT_ID('tempdb..#TEMP6') IS NOT NULL
    DROP TABLE #TEMP6

	IF OBJECT_ID('tempdb..#TEMP5') IS NOT NULL
    DROP TABLE #TEMP5

	IF OBJECT_ID('tempdb..#TEMP4') IS NOT NULL
    DROP TABLE #TEMP4

	IF OBJECT_ID('tempdb..#TEMP3') IS NOT NULL
    DROP TABLE #TEMP3

	IF OBJECT_ID('tempdb..#TEMP2') IS NOT NULL
    DROP TABLE #TEMP2

	IF OBJECT_ID('tempdb..#TEMP1') IS NOT NULL
    DROP TABLE #TEMP1

	IF OBJECT_ID('tempdb..#TEMP') IS NOT NULL
    DROP TABLE #TEMP

	IF OBJECT_ID('tempdb..#Log') IS NOT NULL
    DROP TABLE #Log

     CREATE TABLE #Log  (
		Activity VARCHAR(100)
		,RecordCount INT
		)

	DECLARE @CurrentDate VARCHAR(8) = CONVERT(VARCHAR(8), GETDATE() - 1, 112)
 
 
	SELECT DISTINCT M.prospectno
		,CUID
		,M.Mkrdt CreatedTime
		,branch
		,S.EmployeeName AS MkrId
		,M.ClientStatus
	INTO #TEMP
	FROM SME..tbl_clientmaster M
	INNER JOIN SME..tbl_ChequeDetails Ch ON Ch.prospectno = M.prospectno
	INNER JOIN SME..portfolio_master P ON M.Portfolio = P.Portfolio_Code AND bussiness ='SME'
	INNER JOIN SME..tbl_prospectManagerDetails S ON M.prospectno = S.prospectno AND mapping = 'SaleMapping'
	WHERE Isdisbursed = 'Y'		
		AND CONVERT(VARCHAR(8), ch.HandoverDate, 112) = @CurrentDate

     INSERT INTO #Log VALUES (
		'makrdate and  HandoverDate cases of 84 and 86'
		,@@ROWCOUNT
		)
	

	--Rule any one should present MotherName/FathersName/SpouseName
	DELETE
	FROM #TEMP
	WHERE prospectno IN (
			SELECT DISTINCT T.prospectno
			FROM #TEMP T
			INNER JOIN SME..INDIVIDUAL_MASTER I ON T.prospectno = I.Ind_ProspectNo
			WHERE LEN(ISNULL(I.MotherName, '')) <= 2 AND LEN(ISNULL(I.FathersName, '')) <= 2 AND LEN(ISNULL(I.SpouseName, '')) <= 2
			)

	INSERT INTO #Log VALUES (
		'Records removed where MotherName/FathersName/SpouseName are blank'
		,@@ROWCOUNT)

	SELECT DISTINCT CUID
		,T.prospectno
		,ApplicantType
		,NAME
		,Mobile
		,Email
		,PANNo
		,I.SrNo  AS IndvOrgMasterId
	INTO #TEMP4
	FROM #TEMP T
	INNER JOIN SME..Indv_OrgMaster I ON T.prospectno = I.prospectno
	WHERE ApplicantType <> 'APPLICANT';

	WITH CTE
	AS (
		SELECT ROW_NUMBER() OVER (
				PARTITION BY NAME
				,ProspectNo
				,ApplicantType
				,NAME ORDER BY ProspectNo
					,ApplicantType
					,NAME
					,mobile DESC
				) AS SRNO
			,*
		FROM #TEMP4
		)
	DELETE
	FROM cte
	WHERE SRNO <> 1

	SELECT T.prospectno
		,APPTYPE
		,fName + ' ' + mName + ' ' + lName NAME
		,right('0000000000' + T.cuid, 10) + right('0000000000' + CONVERT(VARCHAR, I.SRNO), 10) CUID
		,T.CUID AS CUIDOriginal
		,CreatedTime
		,Title
		,MTitle
		,FTitle
		,fName
		,mName
		,LNAME
		,FathersName
		,DOB
		,SpouseName
		,IsMarried
		,SEX
		,MotherName
		,Occupation		
		,branch
		,Mobile
		,Email
		,PANNo
		,VotersID
		,DrivingLicense
		,Passport
		,T.MkrId
		,T.ClientStatus
		,Adharno
		,I.IndvOrgMasterId
	INTO #TEMP1
	FROM #TEMP T
	INNER JOIN SME..INDIVIDUAL_MASTER I ON T.prospectno = I.Ind_ProspectNo
	INNER JOIN #TEMP4 INDV ON I.Ind_ProspectNo = INDV.prospectno AND INDV.IndvOrgMasterId = I.IndvOrgMasterId

	
	INSERT INTO #Log VALUES  (
		'Distinct individuals present'
		,@@ROWCOUNT
		)

	DROP TABLE #TEMP4

	--Rule if FathersName is not present then PAN mandatory
	--DELETE
	--FROM #TEMP1
	--WHERE LEN(ISNULL(FathersName, '')) <= 2 AND LEN(ISNULL(PANNo, '')) <> 10;

	

	;WITH CTE
	AS (
		SELECT ROW_NUMBER() OVER (
				PARTITION BY CUID ORDER BY ProspectNo
					,AppType
					,NAME
					,mobile DESC
				) AS SRNO
			,*
		FROM #TEMP1
		)
	DELETE
	FROM CTE
	WHERE SRNO <> 1


	IF (@@ROWCOUNT > 0)
		INSERT INTO #Log
		VALUES (
			'Duplicate records removed'
			,@@ROWCOUNT
			);

	DROP TABLE #TEMP

	--Rule for POA follow sequence of PASSPORT,  DRIVING LICENSE
	SELECT CUID
		,CUIDOriginal
		,CreatedTime
		,Title
		,MTitle
		,FTitle
		,fName
		,mName
		,LNAME
		,FathersName
		,DOB
		,SpouseName
		,IsMarried
		,SEX
		,MotherName
		,Occupation
		,MkrId
		,branch
		,Mobile
		,Email
		,P.PANNo
		,P.VotersID
		,DrivingLicense
		,Passport
		,P.ProspectNo
		,APPTYPE
		,NAME Applicant_Name
		--,C.ParametersName
		,CONVERT(VARCHAR(100), 'Address Proof') AS ParametersName
		,S.ParameterName AS ParameterName
		,D.Extension AS File_Extension
		,D.DocumentPath AS Image_Path
		,CASE   WHEN S.ParameterName = 'PASSPORT' THEN 1 
				WHEN S.ParameterName = 'ELECTION ID CARD' THEN 3
				WHEN S.ParameterName = 'DRIVING LICENSE' THEN 4 
				WHEN S.ParameterName = 'GOVERNMENT ID CARD' THEN 6 
				ELSE 5 END IDProof
		,P.Adharno
	INTO #TEMP2
	FROM #TEMP1 P WITH (NOLOCK)
	INNER JOIN SME..Pl_DocumentUpload D WITH (NOLOCK) ON P.prospectno = D.prospectno AND P.IndvOrgMasterId = D.IndvOrgMasterId
	INNER JOIN SME..tbl_SMETabMaster C WITH (NOLOCK) ON C.srno = D.CatID AND C.STATUS = 1
	INNER JOIN SME..tbl_SMETabSubMaster S WITH (NOLOCK) ON S.ParentId = C.srno 
				AND CAST(S.ParameterId AS VARCHAR(10)) = CAST(D.SubCatID AS VARCHAR(10)) AND S.STATUS = 1
	WHERE D.CatID = 6
	UNION
	SELECT CUID
		,CUIDOriginal
		,CreatedTime
		,Title
		,MTitle
		,FTitle
		,fName
		,mName
		,LNAME
		,FathersName
		,DOB
		,SpouseName
		,IsMarried
		,SEX
		,MotherName
		,Occupation
		,MkrId
		,branch
		,Mobile
		,Email
		,P.PANNo
		,P.VotersID
		,DrivingLicense
		,Passport
		,P.ProspectNo
		,APPTYPE
		,NAME Applicant_Name
		--,C.ParametersName
		,CONVERT(VARCHAR(100), 'Address Proof') AS ParametersName
		,S.SubCatName AS ParameterName
		,D.Extension AS FileExtension
		,D.DocumentPath AS Image_Path
		,CASE WHEN S.SubCatName = 'PASSPORT' THEN 1 
			  WHEN S.SubCatName = 'Voters ID' THEN 3
		WHEN S.SubCatName = 'DRIVING LICENCE' THEN 4 
		ELSE 5 END IDProof
		,P.Adharno
	FROM #TEMP1 P WITH (NOLOCK)
	INNER JOIN SME..Pl_DocumentUpload D WITH (NOLOCK) ON P.prospectno = D.prospectno  AND P.IndvOrgMasterId = D.IndvOrgMasterId
	INNER JOIN SME..PL_CategoryMaster C WITH (NOLOCK) ON C.CategoryID = D.CatID AND C.Status = 1
	INNER JOIN SME..PL_SubCategoryMaster S WITH (NOLOCK) ON S.CategoryID = C.CategoryID 
				AND CAST(S.SubCatID AS VARCHAR(10)) = CAST(D.SubCatID AS VARCHAR(10)) 
	WHERE D.CatID = 2
		
	DELETE
	FROM #TEMP2
	WHERE IDProof = 5 AND ParametersName = 'Address Proof'

	INSERT INTO #Log
	VALUES (
		'Address Proof delete AS NOT OF PASSPORT, DRIVING LICENSE'
		,@@ROWCOUNT
		)



	INSERT INTO #TEMP2
	SELECT CUID
		,CUIDOriginal
		,CreatedTime
		,Title
		,MTitle
		,FTitle
		,fName
		,mName
		,LNAME
		,FathersName
		,DOB
		,SpouseName
		,IsMarried
		,SEX
		,MotherName
		,Occupation
		,MkrId
		,branch
		,Mobile
		,Email
		,P.PANNo
		,P.VotersID
		,DrivingLicense
		,Passport
		,P.ProspectNo
		,APPTYPE
		,NAME Applicant_Name
		--,C.ParametersName
		,CONVERT(VARCHAR(100), 'Correspondence AddressProof') AS ParametersName
		,S.ParameterName AS ParameterName 
		,D.Extension AS File_Extension
		,D.DocumentPath AS Image_Path
		,CASE WHEN S.ParameterName = 'UTILITY BILL' THEN 1
	         ELSE 3 END IDProof
		,P.Adharno
	FROM #TEMP1 P WITH (NOLOCK)
	INNER JOIN SME..Pl_DocumentUpload D WITH (NOLOCK) ON P.prospectno = D.prospectno AND P.IndvOrgMasterId = D.IndvOrgMasterId
	INNER JOIN SME..tbl_SMETabMaster C WITH (NOLOCK) ON C.srno = D.CatID AND C.STATUS = 1
	INNER JOIN SME..tbl_SMETabSubMaster S WITH (NOLOCK) ON S.ParentId = C.srno 
				AND CAST(S.ParameterId AS VARCHAR(10)) = CAST(D.SubCatID AS VARCHAR(10)) AND S.STATUS = 1
	WHERE D.CatID = 6
	UNION
	SELECT CUID
		,CUIDOriginal
		,CreatedTime
		,Title
		,MTitle
		,FTitle
		,fName
		,mName
		,LNAME
		,FathersName
		,DOB
		,SpouseName
		,IsMarried
		,SEX
		,MotherName
		,Occupation
		,MkrId
		,branch
		,Mobile
		,Email
		,P.PANNo
		,P.VotersID
		,DrivingLicense
		,Passport
		,P.ProspectNo
		,APPTYPE
		,NAME Applicant_Name
		--,C.ParametersName
		,CONVERT(VARCHAR(100), 'Correspondence AddressProof') AS ParametersName
		,S.SubCatName AS ParameterName
		,D.Extension AS File_Extension
		,D.DocumentPath AS Image_Path
		,CASE WHEN S.SubCatName = 'UTILITY BILL' THEN 1 
			ELSE 3 END IDProof
		,P.Adharno
	FROM #TEMP1 P WITH (NOLOCK)
	INNER JOIN SME..Pl_DocumentUpload D WITH (NOLOCK) ON P.prospectno = D.prospectno  AND P.IndvOrgMasterId = D.IndvOrgMasterId
	INNER JOIN SME..PL_CategoryMaster C WITH (NOLOCK) ON C.CategoryID = D.CatID AND C.Status = 1
	INNER JOIN SME..PL_SubCategoryMaster S WITH (NOLOCK) ON S.CategoryID = C.CategoryID 
				AND CAST(S.SubCatID AS VARCHAR(10)) = CAST(D.SubCatID AS VARCHAR(10))
	WHERE D.CatID = 2
		
	DELETE
	FROM #TEMP2
	WHERE IDProof = 3 AND ParametersName = 'Correspondence AddressProof'

	INSERT INTO #Log
	VALUES (
		'Correspondence Address Proof delete AS NOT OF PASSPORT, UTILITY BILL '
		,@@ROWCOUNT
		)

	--Rule for POI follow sequence of PHOTO PAN CARD, ELECTION ID CARD, DRIVING LICENSE, PASSPORT
	--INSERT INTO #TEMP2
	--SELECT CUID
	--	,CUIDOriginal
	--	,CreatedTime
	--	,Title
	--	,MTitle
	--	,FTitle
	--	,fName
	--	,mName
	--	,LNAME
	--	,FathersName
	--	,DOB
	--	,SpouseName
	--	,IsMarried
	--	,SEX
	--	,MotherName
	--	,Occupation
	--	,MkrId
	--	,branch
	--	,Mobile
	--	,Email
	--	,P.PANNo
	--	,P.VotersID
	--	,DrivingLicense
	--	,Passport
	--	,P.ProspectNo
	--	,APPTYPE
	--	,NAME Applicant_Name
	--	--,C.ParametersName
	--	,'Identity Proof' AS ParametersName
	--	--,S.ParameterName
	--	,CASE WHEN S.ParameterName IN ('PHOTO PAN CARD', 'BUSINESS PAN', 'PERSONAL PAN', 'Pan card') THEN 'PHOTO PAN CARD' ELSE S.ParameterName END AS ParameterName
	--	,D.File_Extension
	--	,D.Image_Path
	--	,CASE WHEN S.ParameterName IN ('PHOTO PAN CARD', 'BUSINESS PAN', 'PERSONAL PAN', 'Pan card') THEN 1 
	--	WHEN S.ParameterName = 'ELECTION ID CARD' THEN 2 
	--	WHEN S.ParameterName = 'DRIVING LICENSE'
	--	THEN 3 WHEN S.ParameterName = 'PASSPORT' THEN 4 
	--	ELSE 5 END IDProof
	--FROM #TEMP1 P WITH (NOLOCK)
	--INNER JOIN SME..smetab_documents D WITH (NOLOCK) ON P.prospectno = D.prospectno AND REPLACE(D.Applicant_Name, ' ', '') = REPLACE(NAME, ' ', '')
	--INNER JOIN SME..tbl_SMETabMaster C ON C.srno = D.Category AND C.STATUS = 1
	--INNER JOIN SME..tbl_SMETabSubMaster S ON S.ParentId = C.srno AND S.ParameterId = D.Sub_Category AND S.STATUS = 1

	--DELETE
	--FROM #TEMP2
	--WHERE IDProof = 5 AND ParametersName = 'Identity Proof'


	INSERT INTO #TEMP2
	SELECT CUID
		,CUIDOriginal
		,CreatedTime
		,Title
		,MTitle
		,FTitle
		,fName
		,mName
		,LNAME
		,FathersName
		,DOB
		,SpouseName
		,IsMarried
		,SEX
		,MotherName
		,Occupation
		,MkrId
		,branch
		,Mobile
		,Email
		,P.PANNo
		,P.VotersID
		,DrivingLicense
		,Passport
		,P.ProspectNo
		,APPTYPE
		,NAME Applicant_Name
		,C.ParametersName AS ParametersName 
		,S.ParameterName AS ParameterName
		,D.Extension AS File_Extension
		,D.DocumentPath AS Image_Path
		,row_number() OVER (
			PARTITION BY CUID ORDER BY CUID
			) IDProof
		,P.Adharno
	FROM #TEMP1 P WITH (NOLOCK)
	INNER JOIN SME..Pl_DocumentUpload D WITH (NOLOCK) ON P.prospectno = D.prospectno AND P.IndvOrgMasterId = D.IndvOrgMasterId
	INNER JOIN SME..tbl_SMETabMaster C WITH (NOLOCK) ON C.srno = D.CatID AND C.STATUS = 1
	INNER JOIN SME..tbl_SMETabSubMaster S WITH (NOLOCK) ON S.ParentId = C.srno
				 AND CAST(S.ParameterId AS VARCHAR(10)) = CAST(D.SubCatID AS VARCHAR(10)) AND S.STATUS = 1
	WHERE C.ParametersName = 'Photograph'
	UNION
	SELECT CUID
		,CUIDOriginal
		,CreatedTime
		,Title
		,MTitle
		,FTitle
		,fName
		,mName
		,LNAME
		,FathersName
		,DOB
		,SpouseName
		,IsMarried
		,SEX
		,MotherName
		,Occupation
		,MkrId
		,branch
		,Mobile
		,Email
		,P.PANNo
		,P.VotersID
		,DrivingLicense
		,Passport
		,P.ProspectNo
		,APPTYPE
		,NAME Applicant_Name
		,C.CategoryName AS ParametersName
		,S.SubCatName AS ParameterName
		,D.Extension AS File_Extension
		,D.DocumentPath AS Image_Path
		,row_number() OVER (
			PARTITION BY CUID ORDER BY CUID
			) IDProof
		,P.Adharno
	FROM #TEMP1 P WITH (NOLOCK)
	INNER JOIN SME..Pl_DocumentUpload D WITH (NOLOCK) ON P.prospectno = D.prospectno AND P.IndvOrgMasterId = D.IndvOrgMasterId
	INNER JOIN SME..PL_CategoryMaster C WITH (NOLOCK) ON C.CategoryID = D.CatID AND C.Status = 1
	INNER JOIN SME..PL_SubCategoryMaster S WITH (NOLOCK) ON S.CategoryID = C.CategoryID 
				AND CAST(S.SubCatID AS VARCHAR(10)) = CAST(D.SubCatID AS VARCHAR(10))
	WHERE C.CategoryName = 'Photo'

	DELETE
	FROM #TEMP1
	WHERE CUID IN (
			SELECT T1.CUID
			FROM #TEMP1 T1
			LEFT JOIN #TEMP2 T2 ON T1.CUID = T2.CUID
			WHERE T2.CUID IS NULL
			)

	INSERT INTO #Log
	VALUES (
		'Records where images not present'
		,@@ROWCOUNT
		)

	DROP TABLE #TEMP1;

	WITH CTE
	AS (
		SELECT ROW_NUMBER() OVER (
				PARTITION BY CUID
				,ParametersName ORDER BY CUID
					,ParametersName
					,IDProof
				) AS SRNO
			,CUID
			,CUIDOriginal
			,ProspectNo
			,AppType
			,Applicant_Name
			,ParametersName
			,ParameterName
			,File_Extension
			,Image_Path
			,IDProof
			,CreatedTime
			,Title
			,MTitle
			,FTitle
			,fName
			,mName
			,LNAME
			,FathersName
			,DOB
			,SpouseName
			,IsMarried
			,SEX
			,MotherName
			,Occupation
			,MkrId
			,branch
			,Mobile
			,Email
			,PANNo
			,VotersID
			,DrivingLicense
			,Passport
			,Adharno
		FROM #TEMP2
		)
	DELETE
	FROM CTE
	WHERE SRNO <> 1

	--Rule if 3 images againts Photo, POI, POA is preesnet then only proceed else record will get failed at CERSAI end
	SELECT DISTINCT CUID INTO #TEMP6
	FROM #TEMP2
	GROUP BY CUID
	HAVING COUNT(cuid) < 2


	DELETE
	FROM #TEMP2
	WHERE CUID IN (
			SELECT CUID
			FROM #TEMP6
			);

    INSERT INTO #Log
	VALUES (
		-- 'Records removed where POCA, POA and Photo all 3 images not present'
		'Records removed where POA and Photo all 2 images not present'
		,@@ROWCOUNT
		);
  
	DROP TABLE #TEMP6

	SELECT prospectNo
		,CUID
		,CASE WHEN ParametersName = 'Address Proof' AND ParameterName = 'ELECTION ID CARD' THEN 'votercard - address proof'
		 WHEN ParametersName = 'Address Proof' AND ParameterName = 'PASSPORT' THEN 'passport - address proof' 
		 WHEN ParametersName = 'Address Proof' AND ParameterName = 'DRIVING LICENSE' THEN 'drivinglicense - address proof' 
		-- WHEN ParametersName = 'Address Proof' AND ParameterName = 'RATION CARD' THEN 'rationcard'
		  WHEN ParametersName = ' Correspondence AddressProof' AND ParameterName = 'UTILITY BILL' THEN 'Utilitybill2m'
		  WHEN ParametersName = ' Correspondence AddressProof' AND ParameterName = 'PASSPORT' THEN 'passport'
		--  WHEN ParametersName = 'Address Proof' AND ParameterName = 'LIC POLICY OR RECEIPT' THEN 'otherspoackycind'
		   --WHEN ParametersName = 'Address Proof' AND ParameterName = 'LEAVE AND LICENSE AGREEMENT' THEN 'otherspoackycind' 
		   --WHEN ParametersName = 'Identity Proof' AND ParameterName = 'GOVERNMENT ID CARD' THEN 'ckycpois01idnumber' 
		   --WHEN ParametersName = 'Identity Proof' AND ParameterName = 'PHOTO PAN CARD' THEN 'pancard' 
		   WHEN ParametersName = 'Address Proof' AND ParameterName = 'UTILITY BILL' THEN 'Utilitybill2m'
		   --WHEN ParametersName = 'Identity Proof' AND ParameterName = 'ELECTION ID CARD' THEN 'votercard - id proof' 
		   WHEN ParametersName = 'Photograph' AND ParameterName = 'PHOTO' 
				THEN 'photograph' ELSE ParameterName END AS AttachmentCode
		,File_Extension
		,reverse(left(reverse(Image_Path), charindex('\', reverse(Image_Path)) - 1)) AS FiLEName
		,Image_Path
    INTO #CustomerSME
	FROM (
		SELECT DISTINCT ProspectNo
			,CUID
			,Applicant_Name
			,ParametersName
			,ParameterName
			,File_Extension
			,Image_Path
		FROM #TEMP2
		) AS T
		WHERE ParameterName != ''
	
	
	INSERT INTO #Log
	VALUES (
		'Image records for CERSAI'
		,@@ROWCOUNT
		)


	SELECT *
		,(
			SELECT FirstName
			FROM MasterHub.dbo.SplitName(FathersName)
			) AS FatherFirstName
		,(
			SELECT MiddLEName
			FROM MasterHub.dbo.SplitName(FathersName)
			) AS FatherMiddLEName
		,(
			SELECT LastName
			FROM MasterHub.dbo.SplitName(FathersName)
			) AS FatherLastName
		,(
			SELECT FirstName
			FROM MasterHub.dbo.SplitName(SpouseName)
			) AS SpouseFirstName
		,(
			SELECT MiddLEName
			FROM MasterHub.dbo.SplitName(SpouseName)
			) AS SpouseMiddLEName
		,(
			SELECT LastName
			FROM MasterHub.dbo.SplitName(SpouseName)
			) AS SpouseLastName
		,(
			SELECT FirstName
			FROM MasterHub.dbo.SplitName(MotherName)
			) AS MotherFirstName
		,(
			SELECT MiddLEName
			FROM MasterHub.dbo.SplitName(MotherName)
			) AS MotherMiddLEName
		,(
			SELECT LastName
			FROM MasterHub.dbo.SplitName(MotherName)
			) AS MotherLastName
		,CONVERT(VARCHAR(100), '') AS AddressProof
		,CONVERT(VARCHAR(100), '') AS IdentityProof
		,CONVERT(VARCHAR(100), '') AS PhotoProof
	  INTO #TEMP3
	FROM #TEMP2
	ORDER BY ProspectNo
		,AppType
		,Applicant_Name
		,ParametersName
		,ParameterName
		,IDProof

	DROP TABLE #TEMP2

	UPDATE T
	SET AddressProof = T1.ParameterName
	FROM #TEMP3 T
	INNER JOIN #TEMP3 T1 ON T.CUID = T1.CUID
	WHERE T1.ParametersName = 'Address Proof'

	UPDATE T
	SET IdentityProof = T1.ParameterName
	FROM #TEMP3 T
	INNER JOIN #TEMP3 T1 ON T.CUID = T1.CUID
	WHERE T1.ParametersName = 'Correspondence AddressProof'

	UPDATE T
	SET PhotoProof = T1.ParameterName
	FROM #TEMP3 T
	INNER JOIN #TEMP3 T1 ON T.CUID = T1.CUID
	WHERE T1.ParametersName = 'Photograph';

	WITH CTE
	AS (
		SELECT ROW_NUMBER() OVER (
				PARTITION BY CUID ORDER BY CUID
				) AS SRNO
			,CUID
			,CUIDOriginal
			,ProspectNo
			,AppType
			,Applicant_Name
			,ParametersName
			,ParameterName
			,File_Extension
			,Image_Path
			,IDProof
			,CreatedTime
			,Title
			,MTitle
			,FTitle
			,fName
			,mName
			,LNAME
			,FathersName
			,DOB
			,SpouseName
			,IsMarried
			,SEX
			,MotherName
			,Occupation
			,MkrId
			,branch
			,Mobile
			,Email
			,PANNo
			,VotersID
			,DrivingLicense
			,Passport
			,Adharno			
		FROM #TEMP3
		)
	DELETE
	FROM CTE
	WHERE SRNO <> 1


     SELECT DISTINCT 'SME' AS [SourceSystemName]
		,T.CUID AS [SourcesystemCustCode]
		,0 AS [SmallCustomer]
		,0 AS [EkycOTPbased]
		,'New' AS [TransactionType]
		,CASE WHEN ISDATE([CreatedTime]) = 1 THEN FORMAT(CONVERT(DATETIME, [CreatedTime], 6), 'dd-MMM-yyyy') ELSE NULL END AS [SourceSystemCustomerCreationDate]
		,'1' AS [ConstitutionType]
		,T.Title [Prefix]
		,T.fname [FirstName]
		,T.mName [MiddLEName]
		,REPLACE(T.LNAME, '.', ' ') [LastName]
		,'' [MaidenPrefix]
		,'' [MaidenFirstName]
		,'' [MaidenMiddLEName]
		,'' [MaidenLastName]
		,CASE WHEN ISNULL(T.FathersName, '') <> '' THEN (CASE WHEN ISNULL(T.FTitle, '') <> '' THEN FTitleMaster.NAME ELSE 'Mr' END) ELSE '' END [FatherPrefix]
		,REPLACE(T.FatherFirstName, '.', ' ') AS [FatherFirstName]
		,T.FatherMiddLEName
		,T.FatherLastName
		,CASE WHEN ISNULL(T.[SpouseFirstName], '') <> '' THEN (CASE WHEN T.[Sex] = 'M' THEN 'Mrs' ELSE 'Mr' END) ELSE '' END AS [SpousePrefix]
		,T.SpouseFirstName
		,T.SpouseMiddLEName
		,T.SpouseLastName
		,CASE WHEN ISNULL(T.[MotherFirstName], '') <> '' THEN (CASE WHEN ISNULL(T.MTitle, '') <> '' THEN MTitleMaster.NAME ELSE 'Mrs' END) ELSE '' END AS [MotherPrefix]
		,REPLACE(T.[MotherFirstName], '.', '') AS [MotherFirstName]
		,T.MotherMiddLEName
		,T.MotherLastName
		,T.[Sex] AS [Gender]
		,CASE WHEN IsMarried = 'Married' THEN 'M' WHEN IsMarried = 'Single' THEN 'U' ELSE 'O' END AS [MaritalStatus]
		,'IN' AS [Citizenship]
		,CASE WHEN Occupation IN ('Business') THEN 'B-01' WHEN Occupation IN ('Others') THEN 'O-02' WHEN Occupation IN ('SERVICE') THEN 'S-02' ELSE 'X-01' END AS [OccupationType]
		,CASE WHEN ISDATE(T.DOB) = 1 THEN FORMAT(CONVERT(DATETIME, T.DOB), 'dd-MMM-yyyy') ELSE NULL END AS [DateofBirth]
		,'01' AS [ResidentialStatus]
		,'' [EmailId]
		,CASE WHEN ISDATE([CreatedTime]) = 1 THEN FORMAT(CONVERT(DATETIME, [CreatedTime], 6), 'dd-MMM-yyyy') ELSE NULL END AS [KYCDateOfDeclaration]
		,T.branch AS [KYCPlaceOfDeclaration]
		,CASE WHEN ISDATE([CreatedTime]) = 1 THEN FORMAT(CONVERT(DATETIME, [CreatedTime], 6), 'dd-MMM-yyyy') ELSE NULL END AS [KYCVerificationDate]
		,ISNULL(COALESCE(emp.EmpName, ''), '') AS [KYCEmployeeName]
		,ISNULL(COALESCE(emp.Designation, ''), '') AS [KYCEmployeeDesignation]
		,'HO' AS [KYCVerificationBranch]
		,T.MKRID AS [KYCEmployeeCode]
		,'01' AS [PermanentCKYCAddType]
		,'IN' AS [PermanentCountry]
		,COALESCE(CA.AD_PinCode, CA1.AD_PinCode) AS [PermanentPin]
		,COALESCE(CA.ad_Add1, CA1.ad_Add1) AS [PermanentAddressLine1]
		,COALESCE(CA.ad_Add2, CA1.ad_Add2) AS [PermanentAddressLine2]
		,COALESCE(CA.ad_Add3, CA1.ad_Add3) AS [PermanentAddressLine3]
		,'' AS [PermanentDistrict]
		,ISNULL((CASE WHEN ISNULL(cm.City_Description, '') <> '' THEN cm.City_Description ELSE cm1.City_Description END), '') AS [PermanentCity]
		,CASE WHEN ISNULL(COALESCE(SM.State_Code, SM1.State_Code), '') = 'CT' THEN 'CG' WHEN ISNULL(COALESCE(SM.State_Code, SM1.State_Code), '') = 'TG' THEN 'TS' 
		WHEN ISNULL(COALESCE(SM.State_Code, SM1.State_Code), '') = 'UT' THEN 'UA' ELSE ISNULL(COALESCE(SM.State_Code, SM1.State_Code), '') END AS [PermanentState]
		,(CASE WHEN AddressProof = 'PASSPORT' THEN 'Passport' 
		WHEN AddressProof = 'DRIVING LICENSE' THEN 'DrivingLicence' 
		WHEN AddressProof = 'ELECTION ID CARD' THEN 'VoterID'
	 --   WHEN AddressProof = 'RATION CARD' THEN 'RationCard' 
		--WHEN AddressProof = 'LIC POLICY OR RECEIPT' THEN 'OthersPOACKYCInd' 
		--WHEN AddressProof = 'LEAVE AND LICENSE AGREEMENT' THEN 'OthersPOACKYCInd' 
		WHEN AddressProof = 'GOVERNMENT ID CARD' THEN 'AadharCard' --  'OthersPOACKYCInd' 
		--WHEN AddressProof = 'UTILITY BILL' THEN 'Utilitybill2m' 
		ELSE AddressProof END) AS [PermanentAddressProof]
		,'IN' AS [CorrespondenceGlobalCountry]
		,COALESCE(CA.AD_PinCode, CA1.AD_PinCode) AS [CorrespondenceGlobalPin]
		,COALESCE(CA.ad_Add1, CA1.ad_Add1) AS [CorrespondenceGlobalAddressLine1]
		,COALESCE(CA.ad_Add2, CA1.ad_Add2) AS [CorrespondenceGlobalAddressLine2]
		,COALESCE(CA.ad_Add3, CA1.ad_Add3) AS [CorrespondenceGlobalAddressLine3]
		,'' AS [CorrespondenceGlobalDistrict]
		,ISNULL((COALESCE(cm.City_Description, cm1.City_Description, '')), '') AS [CorrespondenceGlobalCity]
		,CASE WHEN ISNULL(COALESCE(SM.State_Code, SM1.State_Code), '') = 'CT' THEN 'CG' 
		WHEN ISNULL(COALESCE(SM.State_Code, SM1.State_Code), '') = 'TG' THEN 'TS'
		 WHEN ISNULL(COALESCE(SM.State_Code, SM1.State_Code), '') = 'UT' THEN 'UA' ELSE ISNULL(COALESCE(SM.State_Code, SM1.State_Code), '') END AS [CorrespondenceGlobalState]
		,'IN' AS [JurisdictionOfResidence]
		,'IN' AS [CountryOfBirth]
		,'' AS [BirthCity]
		,'' AS [TaxIdentificationNumber]
		,'' AS [TaxResidencyAddressLine1]
		,'' AS [TaxResidencyAddressLine2]
		,'' AS [TaxResidencyAddressLine3]
		,'' AS [TaxResidencyPin]
		,'' AS [TaxResidencyDistrict]
		,ISNULL((COALESCE(cm.City_Description, cm1.City_Description)), '') AS [TaxResidencyCity]
		,'' AS [TaxResidencyState]
		,'IN' AS [TaxResidencyCountry]
		,NULL AS [ResidentialSTDCode]
		,NULL AS [ResidentialTelephoneNumber]
		,NULL AS [OfficeSTDCode]
		,NULL AS [OfficeTelephoneNumber]
		,NULL AS [MobileISD]
		,CASE WHEN ISNUMERIC(LEFT([Mobile], 10)) = 1 THEN LEFT([Mobile], 10) ELSE NULL END AS [MobiLENumber]
		,NULL AS [FaxSTD]
		,NULL AS [FaxNumber]
		,NULL AS [CKYCID]
		
		,NULL AS [PassportExpiryDate]
		,T.VotersID AS [VoterIdCard]
		,T.PANNo AS [PAN]
		,T.VotersID
		,T.Passport
		,T.DrivingLicense AS [DrivingLicenseNumber]
		,NULL AS [DrivingLicenseExpiryDate]
		,T.Adharno AS [Aadhaar]
		,'' AS [NREGA]
		,'' AS [CKYCPOIOtherCentralGovtID]
		,'' AS [CKYCPOIS01IDNumber]
		,'' AS [CKYCPOIS02IDNumber]
		--,(CASE WHEN IdentityProof = 'LEGIBLE DRIVING LICENSE' THEN 'DrivingLicence'
		-- WHEN IdentityProof = 'PASSPORT' THEN 'Passport'
		--  WHEN IdentityProof = 'ELECTION ID CARD' THEN 'VoterID' 
		--  WHEN IdentityProof = 'PHOTO PAN CARD' THEN 'PAN' 
		--  WHEN IdentityProof = 'PHOTO RATION CARD' THEN 'OthersPOICKYCInd' ELSE IdentityProof END) 
		 , '' AS [ProofOfIDSubmitted]
		  ,( CASE WHEN AddressProof = 'PASSPORT' THEN 'Passport' 
		          WHEN AddressProof = 'DRIVING LICENSE' THEN 'DrivingLicence' 
		          WHEN AddressProof = 'ELECTION ID CARD' THEN 'VoterID'
		         WHEN IdentityProof = 'UTILITY BILL' THEN 'Utilitybill2m' 
				 WHEN AddressProof = 'GOVERNMENT ID CARD' THEN 'AadharCard'
				 ELSE AddressProof END) AS [CorrespondenceAddressProof]

		,NULL AS [CustomerDemiseDate]
		,0 AS [Minor]
		,'' AS [SourcesystemRelatedPartyCode]
		,'' AS [RelatedPersonType]
		,'' AS [RelatedPersonPrefix]
		,'' AS [RelatedPersonFirstName]
		,'' AS [RelatedPersonMiddLEName]
		,'' AS [RelatedPersonLastName]
		,NULL AS [RelatedPersonCKYCID]
		,'' AS [RelatedPersonPassportNumber]
		,NULL AS [RelatedPersonPassportExpiryDate]
		,'' AS [RelatedPersonVoterIdCard]
		,'' AS [RelatedPersonPAN]
		,'' AS [RelatedPersonDrivingLicenseNumber]
		,NULL AS [RelatedPersonDrivingLicenseExpiryDate]
		,NULL AS [RelatedPersonAadhaar]
		,'' AS [RelatedPersonNREGA]
		,'' AS [RelatedPersonCKYCPOIOtherCentralGovtID]
		,'' AS [RelatedPersonCKYCPOIS01IDNumber]
		,'' AS [RelatedPersonCKYCPOIS02IDNumber]
		,'' AS [RelatedPersonProofOfIDSubmitted]
		,'' AS [SourceSystemSegment]
		,T.ProspectNo AS [AppRefNumberforImages]
		,'' AS [HolderforImages]
		,T.branch BranchCode
		,CASE WHEN ISNULL(T.FatherFirstName, '') <> '' OR ISNULL(T.SpouseFirstName, '') <> '' THEN 'Y' ELSE 'N' END AS [IsValid],
		tcm.ClientStatus AS CustomerStatus,
		T.CreatedTime As CustomerStatusEffectiveDate
		INTO #TEMP5
	FROM #TEMP3 T
	INNER JOIN SME.dbo.tbl_clientMaster tcm WITH(NOLOCK) ON T.ProspectNo = tcm.ProspectNo
	LEFT JOIN  [UCID].[dbo].[Unique_Customers] [U] WITH (NOLOCK) ON [T].[CUIDOriginal] = [U].[CustomerID] AND Business = 'SME'
	LEFT JOIN [Masterhub].[dbo].[EmployeeDataAll] [Emp] WITH (NOLOCK) ON [T].[Mkrid] = [Emp].[EmpId]
	LEFT JOIN SME.dbo.tbl_clientaddresses CA WITH (NOLOCK) ON CA.AD_PROSPECTNO = T.PROSPECTNO AND APPTYPE = CA.ApplicantType AND REPLACE(CA.ApplicantName, ' ', '') = REPLACE(Applicant_Name, ' ', '') AND ca.ad_AddressType = 'PERMANENT RESIDENCE'
	LEFT JOIN SME.dbo.tbl_clientaddresses CA1 WITH (NOLOCK) ON CA1.AD_PROSPECTNO = T.PROSPECTNO AND APPTYPE = CA1.ApplicantType AND REPLACE(CA1.ApplicantName, ' ', '') = REPLACE(Applicant_Name, ' ', '') AND CA1.ad_AddressType = 'CURRENT RESIDENCE'
	LEFT JOIN SME.dbo.City_Master cm WITH (NOLOCK) ON cm.City_Code = CA.ad_City OR cm.CityId = CA.ad_City
	LEFT JOIN SME.dbo.state_master SM WITH (NOLOCK) ON SM.State_Code = CA.ad_State
	LEFT JOIN SME.dbo.City_Master cm1 WITH (NOLOCK) ON cm1.City_Code = CA1.ad_City OR cm1.CityId = CA1.ad_City
	LEFT JOIN SME.dbo.state_master SM1 WITH (NOLOCK) ON SM1.State_Code = CA1.ad_State
	LEFT JOIN Masterhub..LOV FTitleMaster WITH (NOLOCK) ON FTitleMaster.Value = T.FTitle AND FTitleMaster.ListType = 'Title'
	LEFT JOIN Masterhub..LOV MTitleMaster WITH (NOLOCK) ON MTitleMaster.Value = T.MTitle AND MTitleMaster.ListType = 'Title'



	INSERT INTO #Log
	VALUES (
		'Data records for CERSAI'
		,@@ROWCOUNT
		)


		INSERT INTO Clients.dbo.StagingCustom217CustomerFlat(
				  ParentCompany
				,TransactionID
				,SourceSystemName
				,SourceSystemCustomerCode
				,SourceSystemCustomerCreationDate
				,IsSmallCustomer
				,EkycOTPbased
				,RecordIdentifier
				,Segments
				,SegmentStartDate
				,ProductSegments
				,CustomerStatus
				,CustomerStatusEffectiveDate
				,RelatedPartyStatus
				,RelatedPartyStatusEffectiveDate
				,CustomerType
				,CustomerSubType
				,Prefix
				,FirstName
				,MiddleName
				,LastName
				,MaidenPrefix
				,MaidenFirstName
				,MaidenMiddleName
				,MaidenLastName
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
				,OccupationType
				,ActivitySector
				,NatureOfBusiness
				,NatureOfBusinessOther
				,DateofBirth
				,WorkEmail
				,PersonalEmail
				,KYCDateOfDeclaration
				,KYCPlaceOfDeclaration
				,KYCVerificationDate
				,KYCEmployeeName
				,KYCEmployeeDesignation
				,KYCVerificationBranch
				,KYCEmployeeCode
				,PermanentCKYCAddressType
				,PlotnoSurveynoHouseFlatno
				,PermanentAddressCountry
				,PermanentAddressPinCode
				,PermanentAddressLine1
				,PermanentAddressLine2
				,PermanentAddressLine3
				,PermanentAddressDistrict
				,PermanentAddressCity
				,PermanentAddressState
				,PermanentAddressProof
				,CorrespondenceAddressCountry
				,CorrespondenceAddressPinCode
				,CorrespondenceAddressLine1
				,CorrespondenceAddressLine2
				,CorrespondenceAddressLine3
				,CorrespondenceAddressDistrict
				,CorrespondenceAddressCity
				,CorrespondenceAddressState
				,CorrespondenceAddressProof
				,WorkAddressCountry
				,WorkAddressPinCode
				,WorkAddressLine1
				,WorkAddressLine2
				,WorkAddressLine3
				,WorkAddressDistrict
				,WorkAddressCity
				,WorkAddressState
				,CountryOfBirth
				,BirthCity
				,TaxResidencyCountry
				,TaxIdentificationNumber
				,TaxResidencyAddressCountry
				,TaxResidencyAddressLine1
				,TaxResidencyAddressLine2
				,TaxResidencyAddressLine3
				,TaxResidencyAddressPinCode
				,TaxResidencyAddressDistrict
				,TaxResidencyAddressCity
				,TaxResidencyAddressState
				,DeskPersonalISDCode
				,DeskPersonalSTDCode
				,DeskPersonalTelephoneNumber
				,DeskWorkISDCode
				,DeskWorkSTDCode
				,DeskWorkTelephoneNumber
				,WorkMobileISD
				,WorkMobileNumber
				,PersonalMobileISD
				,PersonalMobileNumber
				,CKYCID
				,PassportIssueCountry
				,PassportNumber
				,PassportExpiryDate
				,VoterIdCard
				,PAN
				,DrivingLicenseNumber
				,DrivingLicenseExpiryDate
				,Aadhaar
				,AadhaarVaultReferenceNumber
				,AadhaarToken
				,AadhaarVirtualId
				,NREGA
				,CKYCPOIOtherCentralGovtID
				,CKYCPOIS01IDNumber
				,CKYCPOIS02IDNumber
				,NationalID
				,TaxIdLocal
				,CompanyRegistrationNumber
				,CompanyRegistrationCountry
				,GIIN
				,OthersInd
				,OthersNonInd
				,ProofOfIDSubmitted
				,Minor
				,ApplicationRefNumber
				,HolderforImages
				,IntermediaryCode
				,Listed
				,Industry
				,Nationality
				,CountryofOperation
				,RegulatoryAMLRisk
				,RegAMLSpecialCategory
				,RegAMLSpecialCategoryStartDate
				,RegAMLSpecialCategoryEndDate
				,LastRiskReviewDate
				,NextRiskReviewDate
				,IncomeRange
				,ExactIncome
				,IncomeCurrency
				,IncomeEffectiveDate
				,IncomeDescription
				,IncomeProof
				,ExactNetworth
				,NetworthCurrency
				,NetworthEffectiveDate
				,NetworthDescription
				,NetworthProof
				,PEP
				,PEPClassification
				,AdverseMedia
				,AdverseMediaClassification
				,AdverseMediaDetails
				,InsiderInformation
				,Tags
				,FamilyCode
				,Channel
				,Links
				,ReputationClassification
				,IUPartyType
				,PropertyOwnerFlag
				,ContactPersonFirstName
				,ContactPersonMiddleName
				,ContactPersonLastName
				,ContactPersonDesignation
				,ContactPersonMobileISD
				,ContactPersonMobileNo
				,ContactPersonMobileISD2
				,ContactPersonMobileNo2
				,ContactPersonEmailId1
				,ContactPersonEmailId2
				,RMUserCode
				,RMType
				,RMFromDate
				,EducationalQualification
				,DesignationOthers
				,EmployerName
				,EmployerAddress
				,EmployerListed
				,EmployerOrganisationType
				,CurrentEmploymentInYears
				,ModuleApplicable
				,AddedBy
				,AddedOn
				,GUID
				,FormSixty
				,NPRLetter
				,KYCAttestationType
				,batchId
				,IsValid
				)


      SELECT   
	           'IIFL12' AS ParentCompany,       
	           TransactionID,
               [SourceSystemName],
               SourcesystemCustCode AS [SourceSystemCustomerCode],
			   [SourceSystemCustomerCreationDate],
               0 AS [IsSmallCustomer],
               0 AS [EkycOTPbased],
			   '' AS RecordIdentifier,
			   '' AS Segments,
			   '' AS SegmentStartDate,
			   '' AS ProductSegments,
			   CASE WHEN CustomerStatus = 'ACT' THEN 'Active'
			    WHEN CustomerStatus = 'CLS' THEN 'Closed'
				ELSE 'Suspended' END AS CustomerStatus,
			   CustomerStatusEffectiveDate,
			   '' RelatedPartyStatus,
			   '' RelatedPartyStatusEffectiveDate,
               [ConstitutionType] AS CustomerType,
			   '' CustomerSubType,
               [Prefix],
               [FirstName],
               [MiddleName],
               [LastName],
               [MaidenPrefix],
               [MaidenFirstName],
               [MaidenMiddleName],
               [MaidenLastName],
               [FatherPrefix],
               [FatherFirstName],
               [FatherMiddleName],
               [FatherLastName],
               [SpousePrefix],
               [SpouseFirstName],
               [SpouseMiddleName],
               [SpouseLastName],
               [MotherPrefix],
               [MotherFirstName],
               [MotherMiddleName],
               [MotherLastName],
               [Gender],
               [MaritalStatus],
               [Citizenship],
			    '' AS CountryOfResidence,
			   [OccupationType],
				'' AS ActivitySector,
				'' AS NatureOfBusiness,
                '' AS NatureOfBusinessOther,
               [DateofBirth],
			    ''   AS WorkEmail,
               [EmailId] AS PersonalEmail,
               [KYCDateOfDeclaration],
               [KYCPlaceOfDeclaration],
               [KYCVerificationDate],
               [KYCEmployeeName],
      [KYCEmployeeDesignation],
               [KYCVerificationBranch],
               [KYCEmployeeCode],
               Final.PermanentCKYCAddType AS [PermanentCKYCAddressType],
       '' AS PlotnoSurveynoHouseFlatno,
			   [PermanentCountry] AS PermanentAddressCountry,
               [PermanentPin] AS PermanentAddressPinCode,
               CASE
                   WHEN ISNULL([PermanentAddressLine1], '') = ''
                        AND ISNULL([PermanentAddressLine2], '') != '' THEN
                       ISNULL([PermanentAddressLine2], '')
                   WHEN ISNULL([PermanentAddressLine1], '') = ''
                        AND ISNULL([PermanentAddressLine2], '') = ''
             AND ISNULL([PermanentAddressLine3], '') != '' THEN
                       ISNULL([PermanentAddressLine3], '')
                   ELSE
                       ISNULL([PermanentAddressLine1], '')
               END AS [PermanentAddressLine1],
               CASE
                   WHEN ISNULL([PermanentAddressLine1], '') = ''
                        AND ISNULL([PermanentAddressLine2], '') != '' THEN
                       ''
                   ELSE
          ISNULL([PermanentAddressLine2], '')
               END AS [PermanentAddressLine2],
              CASE
              WHEN ISNULL([PermanentAddressLine1], '') = ''
                        AND ISNULL([PermanentAddressLine2], '') = ''
                        AND ISNULL([PermanentAddressLine3], '') != '' THEN
                       ''
                   ELSE
                       ISNULL([PermanentAddressLine3], '')
     END AS [PermanentAddressLine3],

               [PermanentDistrict] AS PermanentAddressDistrict,
               dbo.RemoveRepeatingChars(dbo.RemoveSpecialCharacters([PermanentCity])) AS [PermanentAddressCity],
               [PermanentState] AS PermanentAddressState,
               [PermanentAddressProof],
               [CorrespondenceGlobalCountry] AS CorrespondenceAddressCountry,
               [CorrespondenceGlobalPin] AS CorrespondenceAddressPinCode,
               Final.CorrespondenceGlobalAddressLine1 AS [CorrespondenceAddressLine1],
               Final.CorrespondenceGlobalAddressLine2 AS [CorrespondenceAddressLine2],
               Final.CorrespondenceGlobalAddressLine3 AS [CorrespondenceAddressLine3],
               [CorrespondenceGlobalDistrict] AS CorrespondenceAddressDistrict,
               [CorrespondenceGlobalCity] AS CorrespondenceAddressCity,
               [CorrespondenceGlobalState] AS CorrespondenceAddressState,
			   CorrespondenceAddressProof AS CorrespondenceAddressProof,
			   '' AS WorkAddressCountry,
			   '' AS WorkAddressPinCode,
			   '' AS WorkAddressLine1,
			   '' AS WorkAddressLine2,
			   '' AS WorkAddressLine3,
			   '' AS WorkAddressDistrict,
			   '' AS WorkAddressCity,
			   '' AS WorkAddressState,
			   [CountryOfBirth],
               [BirthCity],
               [JurisdictionOfResidence] AS TaxResidencyCountry ,
               [TaxIdentificationNumber],
			   [TaxResidencyCountry] AS TaxResidencyAddressCountry,
               [TaxResidencyAddressLine1] AS TaxResidencyAddressLine1,
               [TaxResidencyAddressLine2] AS TaxResidencyAddressLine2,
               [TaxResidencyAddressLine3] AS TaxResidencyAddressLine3,
               [TaxResidencyPin] AS TaxResidencyAddressPinCode,
               [TaxResidencyDistrict] AS TaxResidencyAddressDistrict,
               dbo.RemoveRepeatingChars(dbo.RemoveSpecialCharacters(ISNULL([TaxResidencyCity], ''))) AS [TaxResidencyAddressCity],
               [TaxResidencyState] AS TaxResidencyAddressState,
			   '' AS DeskPersonalISDCode,
               [ResidentialSTDCode] AS DeskWorkSTDCode,
               [ResidentialTelephoneNumber] AS DeskPersonalTelephoneNumber,
			   '' AS DeskWorkISDCode,
               [OfficeSTDCode] AS DeskWorkSTDCode,
               [OfficeTelephoneNumber] AS DeskWorkTelephoneNumber,
			   '' AS  [WorkMobileISD],
               '' AS  [WorkMobileNumber],
               [MobileISD] AS PersonalMobileISD,
               [MobileNumber] AS PersonalMobileNumber,
               Final.CKYCID AS [CKYCNumber],
               'IN' AS PassportIssueCountry,
              Passport AS PassportNumber, 
             [PassportExpiryDate],
              VoterIdCard AS VoterIdCard,
               [PAN] AS PAN, 
               [DrivingLicenseNumber] AS DrivingLicenseNumber,
               [DrivingLicenseExpiryDate]  AS DrivingLicenseExpiryDate,
              [Aadhaar] AS [Aadhaar],
			   '' AadhaarVaultReferenceNumber,
			   '' AadhaarToken,
			   '' AadhaarVirtualId,
               [NREGA],
               [CKYCPOIOtherCentralGovtID],
               [CKYCPOIS01IDNumber],
               [CKYCPOIS02IDNumber],
			   '' AS NationalID,
			   '' AS TaxIdLocal,
			   '' AS CompanyRegistrationNumber, 
			   '' AS CompanyRegistrationCountry, 
			   '' AS GIIN,
			   '' AS OthersInd,
			   '' AS OthersNonInd,
               [ProofOfIDSubmitted],
               Final.Minor AS [Minor],
			   Final.AppRefNumberforImages AS ApplicationRefNumber,
			   [HolderforImages],
			   [BranchCode] AS IntermediaryCode,
			     NULL AS Listed
				,NULL AS Industry
				,NULL AS Nationality
				,NULL AS CountryofOperation
				,NULL AS RegulatoryAMLRisk
				,NULL AS RegAMLSpecialCategory
				,NULL AS RegAMLSpecialCategoryStartDate
				,NULL AS RegAMLSpecialCategoryEndDate
				,NULL AS LastRiskReviewDate
				,NULL AS NextRiskReviewDate
				,NULL AS IncomeRange
				,NULL AS ExactIncome
				,NULL AS IncomeCurrency
				,NULL AS IncomeEffectiveDate
				,NULL AS IncomeDescription
				,NULL AS IncomeProof
				,NULL AS ExactNetworth
				,NULL AS NetworthCurrency
				,NULL AS NetworthEffectiveDate
				,NULL AS NetworthDescription
				,NULL AS NetworthProof
				,NULL AS PEP
				,NULL AS PEPClassification
				,NULL AS AdverseMedia
				,NULL AS AdverseMediaClassification
				,NULL AS AdverseMediaDetails
				,NULL AS InsiderInformation
				,NULL AS Tags
				,NULL AS FamilyCode
				,NULL AS Channel
				,NULL AS Links
				,NULL AS ReputationClassification
				,NULL AS IUPartyType
				,NULL AS PropertyOwnerFlag
				,NULL AS ContactPersonFirstName
				,NULL AS ContactPersonMiddleName
				,NULL AS ContactPersonLastName
				,NULL AS ContactPersonDesignation
				,NULL AS ContactPersonMobileISD
				,NULL AS ContactPersonMobileNo
				,NULL AS ContactPersonMobileISD2
				,NULL AS ContactPersonMobileNo2
				,NULL AS ContactPersonEmailId1
				,NULL AS ContactPersonEmailId2
				,NULL AS RMUserCode
				,NULL AS RMType
				,NULL AS RMFromDate
				,NULL AS EducationalQualification
				,NULL AS DesignationOthers
				,NULL AS EmployerName
				,NULL AS EmployerAddress
				,NULL AS EmployerListed
				,NULL AS EmployerOrganisationType
				,NULL AS CurrentEmploymentInYears
				,'CKYC' AS ModuleApplicable
				,KYCEmployeeCode AS AddedBy,
				GETDATE () AS AddedOn
				,'' AS GUID
				,0 AS FORMSixty
				,'' AS NPRLetter
				,'01' AS KYCAttestationType
				,@batchId AS batchId
				,'Y' AS IsValid


	FROM (
		SELECT 'SME' + CONVERT(VARCHAR(10), GETDATE(), 112) + CONVERT(VARCHAR(2), DatePart(hour, getdate())) + CONVERT(VARCHAR(2), DatePart(MINUTE, getdate())) + RIGHT('0000000' + CONVERT(VARCHAR(10), ROW_NUMBER() OVER (
						ORDER BY [SourcesystemCustCode]
						)), 10) AS [TransactionID]
			,#TEMP5.*
		FROM #TEMP5
		) AS Final

		INSERT INTO dbo.CKYCDataUploadLog(processname,methodname,activity,CreatedOn,RecordsProcessed,BatchId,Response)
		VALUES('SME Ckyc Data Upload','proc_CKYC_Customer217DataUploadSME','Data upload',GETDATE(),@@ROWCOUNT,@BatchId,'Data is uploaded in StagingCustom217CustomerFlat table.')

		----Validation for the ckyc data
        --UPDATE StagingCustom217CustomerFlat SET IsValid = 'N',
        --    InvalidColRemarks = (CASE
        --                             WHEN dbo.RemoveNonAlphaCharacters(ISNULL(FirstName, '')) = '' THEN
        --                                 'First Name is required field.'
        --                        WHEN
        --             (
        --                                 dbo.RemoveNonAlphaCharacters(ISNULL(SpouseFirstName, '')) = ''
        --                                 AND dbo.RemoveNonAlphaCharacters(ISNULL(FatherFirstName, '')) = ''
        --                                 AND dbo.RemoveNonAlphaCharacters(ISNULL(MotherFirstName, '')) = ''
        --                             ) THEN
        --                                 'Father, Spouse, Mother at least one is required field'
        --                             WHEN
        --                             (
        --                                 dbo.RemoveNonAlphaCharacters(ISNULL(FatherFirstName, '')) = ''
        --                                 AND dbo.RemoveNonAlphaCharacters(ISNULL(PAN, '')) = ''
        --                                 AND
        --                                 (
        --                                     dbo.RemoveNonAlphaCharacters(ISNULL(SpouseFirstName, '')) <> ''
        --                                     OR dbo.RemoveNonAlphaCharacters(ISNULL(MotherFirstName, '')) <> ''
        --                       )
        --                             ) THEN
        --                                 'PAN is required field.'
        --                             WHEN ISNULL(KYCEmployeeName, '') = '' THEN
        --                                 'KYC EmployeeName is required field.'
        --                             WHEN ISNULL(KYCEmployeeDesignation, '') = '' THEN
        --                                 'KYCEmployeeDesignation is required field.'
        --             WHEN dbo.RemoveSpecialCharacters(PermanentAddressCity) = '' THEN
--                           'PermanentCity is required field.'
        --                             WHEN dbo.RemoveSpecialCharacters(PermanentAddressPinCode) = '' THEN
        --                                 'PermanentPin is required field.'
        --                             WHEN LEN(ISNULL(PermanentAddressPinCode, '')) <> 6 THEN
        --                                 'Valid PermanentPin is required.'
        --                             WHEN dbo.RemoveSpecialCharacters(CorrespondenceAddressPinCode) = '' THEN
        --                                 'CorrespondenceGlobalPin is required field.'
        --                             WHEN LEN(ISNULL(CorrespondenceAddressPinCode, '')) <> 6 THEN
        --                                 'Valid CorrespondenceGlobalPin is required.'
        --                             WHEN dbo.RemoveSpecialCharacters(CorrespondenceAddressCity) = '' THEN
        --                                 'CorrespondenceGlobalCity is required field.'
                                    
        --                             WHEN ISNULL(Aadhaar, '') <> ''
        --                                  AND Aadhaar NOT LIKE '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]' THEN
        --                          'Valid aadhar is required.'
        --                      WHEN dbo.RemoveSpecialCharacters(dbo.RemoveRepeatingChars(PermanentAddressLine1 + PermanentAddressLine2 + PermanentAddressLine3
        --                                                                                      )
        --                                                             ) = '' THEN
        --                                 'PermanentAddress is required.'
        --                             WHEN dbo.RemoveSpecialCharacters(dbo.RemoveRepeatingChars(CorrespondenceAddressLine1+ CorrespondenceAddressLine2 + CorrespondenceAddressLine3
        --                                                                                      )
        --                                                             ) = '' THEN
        --                  'CorrespondenceGlobalAddrLine is required.'
        --                             WHEN DrivingLicenseExpiryDate < GETDATE() THEN
        --                  'Driving License is expired.'
        --                             --when ProofOfIDSubmitted='Aadhar'  and  isnull(Aadhaar,'')='' then 'Aadhar is required.'
        --                             --when ProofOfIDSubmitted='PAN'  and  isnull(PAN,'')='' then 'PAN is required.'
        --                             --when ProofOfIDSubmitted='Passport'  and  isnull(PassportNumber,'')='' then 'Passport is required.'
        --                             --when ProofOfIDSubmitted='VoterID'  and  isnull(VoterIdCard,'')='' then 'VoterID is required.'
        --                             --when ProofOfIDSubmitted='DrivingLicence'  and  isnull(DrivingLicenseNumber,'')='' then 'DrivingLicence is required.' 
        --                             ELSE
        --                                 ''
        --                         END
        --                        )
        --WHERE SourceSystemName = 'SME' AND CustomerIntegrationStatus IS NULL AND batchId = @batchId AND
        --      ( (dbo.RemoveNonAlphaCharacters(ISNULL(FirstName, '')) = '') OR
        --          (
       --              dbo.RemoveNonAlphaCharacters(ISNULL(SpouseFirstName, '')) = ''
        --              AND dbo.RemoveNonAlphaCharacters(ISNULL(FatherFirstName, '')) = ''
        --              AND dbo.RemoveNonAlphaCharacters(ISNULL(MotherFirstName, '')) = ''
        --          )
        --          OR
        --          (
        --              dbo.RemoveNonAlphaCharacters(ISNULL(FatherFirstName, '')) = ''
        --              AND dbo.RemoveNonAlphaCharacters(ISNULL(PAN, '')) = ''
        --              AND
        --              (
--                  dbo.RemoveNonAlphaCharacters(ISNULL(SpouseFirstName, '')) <> ''
        --                  OR dbo.RemoveNonAlphaCharacters(ISNULL(MotherFirstName, '')) <> ''
        --              )
        --          )
        --          OR (ISNULL(KYCEmployeeName, '') = '')
        --          OR (ISNULL(KYCEmployeeDesignation, '') = '')
        --          OR (dbo.RemoveSpecialCharacters(PermanentAddressCity) = '')
        --          OR
        --          (
        --              dbo.RemoveSpecialCharacters(PermanentAddressPinCode) = ''
        --              OR LEN(ISNULL(PermanentAddressPinCode, '')) <> 6
        --          )
  --          OR
        --          (
        --              dbo.RemoveSpecialCharacters(CorrespondenceAddressPinCode) = ''
        --              OR LEN(ISNULL(CorrespondenceAddressPinCode, '')) <> 6
        --          )
        --          OR (dbo.RemoveSpecialCharacters(CorrespondenceAddressCity) = '')
        --          OR
        --          (
        --              ISNULL(Aadhaar, '') <> ''
        --              AND Aadhaar NOT LIKE '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
        --          )
        --          OR (dbo.RemoveSpecialCharacters(dbo.RemoveRepeatingChars(PermanentAddressLine1
        --                                                                   + PermanentAddressLine2
        --                                                                   + PermanentAddressLine3
        --                                                                  )
        --                 ) = ''
        --             )
        --          OR (dbo.RemoveSpecialCharacters(dbo.RemoveRepeatingChars(CorrespondenceAddressLine1 + CorrespondenceAddressLine2 + CorrespondenceAddressLine3
        --                                                                  )
        --                                         ) = ''
        --             )
        --      );




		--- IF permanent address is same as Correspondence Addresses
	 --   SELECT  DISTINCT SourceSystemCustomerCode INTO #same_Address
  --      FROM Clients.dbo.StagingCustom217CustomerFlat A WITH(NOLOCK)  
  --      WHERE    A.SourceSystemName = 'SME' AND IsValid = 'Y' AND  A.batchId = @batchId 
  --     AND ISNULL(CorrespondenceAddressPinCode,'') =  ISNULL(PermanentAddressPinCode,'')
	 --AND  ISNULL(PermanentAddressLine1,'') + ' ' + ISNULL(PermanentAddressLine2,'') + ' ' + ISNULL(CorrespondenceAddressLine1,'') =  ISNULL(CorrespondenceAddressLine2,'') + ' ' + ISNULL(PermanentAddressLine2,'') + ' ' + ISNULL(CorrespondenceAddressLine3,'')



  --      -- Update for the same address proof 
	 --   UPDATE s217 SET CorrespondenceAddressProof = PermanentAddressProof
	 --   FROM Clients.dbo.StagingCustom217CustomerFlat s217 
  --      INNER JOIN #same_Address addr ON s217.SourceSystemCustomerCode = addr.SourceSystemCustomerCode
		--WHERE    s217.SourceSystemName = 'SME' AND IsValid = 'Y' AND  s217.batchId = @BatchId 
      
	 --  -- Delete same address proof for permanent and Correspondence 
	 --   DELETE  #CustomerSME WHERE CUID in (SELECT SourceSystemCustomerCode FROM #same_Address)
	    
		--================================================
        
		
	    SELECT StagingCustom217CustomerFlatId, SourceSystemCustomerCode, ApplicationRefNumber, InvalidColRemarks      INTO #tempInValidRecord  
			  FROM (

				SELECT StagingCustom217CustomerFlatId,
					   SourceSystemCustomerCode,
					   ApplicationRefNumber,
					   (CASE
							WHEN
							(
								ISNULL(PermanentAddressProof, '') = ''
								OR ISNULL(PermanentAddressProof, '') LIKE '%PAN%'
							) THEN
								'Address proof is required.'
							WHEN PermanentAddressProof LIKE '%Aadhar%'
								 AND ISNULL(Aadhaar, '') = '' THEN
								'Aadhar number is not provided for uploaded image.'
							--WHEN PermanentAddressProof LIKE '%PAN%' THEN
							--	'Pan is invalid address proof'
							WHEN PermanentAddressProof LIKE '%Passport%'
								 AND ISNULL(PassportNumber, '') = '' THEN
								'Passport number not provided for uploaded image'
							WHEN PermanentAddressProof LIKE '%VoterID%'
								 AND ISNULL(VoterIdCard, '') = '' THEN
								'Voter number not provided for uploaded image'
							  WHEN PermanentAddressProof LIKE '%Driving%'
								AND ISNULL(DrivingLicenseNumber, '') = '' THEN
								'Driving Licence number not provided for uploaded image'
							ELSE
							''
						END
					   ) AS InvalidColRemarks
				FROM Clients.dbo.StagingCustom217CustomerFlat WITH (NOLOCK)
				WHERE batchId = @BatchId AND 
					  SourceSystemName = 'SME'
					  AND CustomerIntegrationStatus IS NULL
					  AND IsValid = 'Y'
					  AND
					  (
						  (
							  ISNULL(PermanentAddressProof, '') = ''
							  OR ISNULL(PermanentAddressProof, '') LIKE '%PAN%'
						  )
						  OR
						  (
							  PermanentAddressProof LIKE '%Aadhar%'
							  AND ISNULL(Aadhaar, '') = ''
						  )
						  --OR
						  --(
							 -- PermanentAddressProof LIKE '%PAN%'
						  --)
						  OR
						  (
							  PermanentAddressProof LIKE '%Passport%'
							  AND ISNULL(PassportNumber, '') = ''
						  )
						  OR
						  (
							  PermanentAddressProof LIKE '%VoterID%'
							  AND ISNULL(VoterIdCard, '') = ''
						  )
						  OR
						  (
							  PermanentAddressProof LIKE '%Driving%'
							  AND ISNULL(DrivingLicenseNumber, '') = ''
						  )
					  )
		
				 UNION ALL
		
				 SELECT StagingCustom217CustomerFlatId,
					   SourceSystemCustomerCode,
					   ApplicationRefNumber,
					   'CorrespondenceAddressProof address images not found' AS InvalidColRemarks
				 FROM StagingCustom217CustomerFlat s217 WITH(NOLOCK)
				 WHERE  s217.SourceSystemName = 'SME' AND IsValid = 'Y'  and  ISNULL(CorrespondenceAddressProof,'') ='' AND batchId = @BatchId 
				) 
				AS T

 -- ---- Update the invalid record 
	    UPDATE a SET a.IsValid = 'N', a.InvalidColRemarks = t.InvalidColRemarks
        FROM Clients.dbo.StagingCustom217CustomerFlat a WITH (NOLOCK)
       INNER JOIN #tempInValidRecord t ON a.StagingCustom217CustomerFlatId = t.StagingCustom217CustomerFlatId
        WHERE batchId = @BatchId AND SourceSystemName = 'SME' AND IsValid = 'Y';	
		
		DELETE FROM #CustomerSME WHERE CUID IN( SELECT SourceSystemCustomerCode FROM #tempInValidRecord);




     ---========================Customer image populdation ===================================
	INSERT INTO CustomerImagesSME (
		TransactionId
		,FiLEName
		,FilePath
		,Filetype
		,DocumentType
		,AppRefNoForImages
		,Cuid
		,BinaryFormat
		,Product
		,IsUploaded
		,mkrid
		,AddedOn
		,Batchid
		)
	SELECT TransactionId
		,REPLACE(REPLACE(FiLEName, '''', ''), '.png', '.jpg') AS FiLEName
		,FilePath
		,FileType
		,lower(AttachmentCode) AS documenttype
		,ProspectNo AS AppRefNoForImages
		,Cuid AS CUID
		,'N' AS BinaryFormat
		,'SME' AS Product
		,'N' AS IsUploaded
		,mkrid
		,Getdate() AS AddedOn
		,CONVERT(VARCHAR(100), @BatchId) as Batchid
	FROM (
		SELECT Rowsnumber + srno AS Transactionid
			,AttachmentCode
			,ProspectNo
			,Cuid
			,FilePath
			,FileType
			,CASE WHEN AttachmentCode LIKE '%photo%' THEN 'PH_' + CONVERT(VARCHAR, (CUID)) + FileType ELSE --+  Upper(substring(AttachmentCode,0,4))
					Upper(substring(AttachmentCode, 0, 4)) + CONVERT(VARCHAR, Rowsnumber) + '_' + CONVERT(VARCHAR, CUID) + FileType END AS FiLEName
			,Mkrid
		FROM (
			SELECT ROW_NUMBER() OVER (
					ORDER BY a.cuid
					) AS Rowsnumber
				,ROW_NUMBER() OVER (
					PARTITION BY a.CUID ORDER BY AttachmentCode
					) rn
				,c.srno
				,a.ProspectNo
				,AttachmentCode
				,a.file_Extension AS FileType
				,a.FiLEName
				,image_Path AS FilePath
				,a.CUID
				,c.Mkrid
			FROM #CustomerSME a
			INNER JOIN SME.dbo.tbl_clientmaster c WITH (NOLOCK) ON a.prospectNo = c.ProspectNo
			--LEFT JOIN CustomerImagesSME SME WITH (NOLOCK) ON a.prospectNo = SME.AppRefNoForImages
			--WHERE a.cuid IN (
			--		SELECT SourceSystemCustomerCode
			--		FROM #customerUpload
			--		) AND SME.Cuid IS NULL
			) AS test
		) AS Final

		INSERT INTO dbo.CKYCDataUploadLog(processname,methodname,activity,CreatedOn,RecordsProcessed,BatchId,Response)
		VALUES('SME Ckyc Data Upload','proc_CKYC_Customer217DataUploadSME','Image Data upload',GETDATE(),@@ROWCOUNT,@BatchId,'Image data is uploaded in CustomerImagesSME table.')



	SELECT CONVERT(VARCHAR(100), a.Srno) AS ID
		,FiLEName NAME
		,FilePath
		,lower(Filetype) AS Filetype
		,DocumentType
		,a.AppRefNoForImages
		,a.Cuid CUID
		,mkrid
		,'N' BinaryFormat
		,NULL AS ClientImage
		,BatchId
		,Product
	FROM CustomerImagesSME a WITH (NOLOCK)
	WHERE  a.Product = 'SME'  AND a.IsUploaded = 'N'  AND Batchid =@BatchId 

	INSERT INTO dbo.CKYCDataUploadLog(processname,methodname,activity,CreatedOn,RecordsProcessed,BatchId,Response)
	VALUES('SME Ckyc Data Upload','proc_CKYC_Customer217DataUploadSME','Process-End',GETDATE(),0,@BatchId,'Process-End')

END TRY
BEGIN CATCH
	DECLARE @ID VARCHAR(15)= CAST(@BatchId AS varchar)
	EXECUTE proc_SaveDBError @ID
END CATCH
END











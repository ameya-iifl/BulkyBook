/*
JK- case when for form60
*/
CREATE PROCEDURE dbo.CKYC_Customer217DataUpload @Product VARCHAR(50)
	,@BatchId INT = 0
AS
BEGIN
	--Check again later
	--DECLARE @CurrentDate VARCHAR(8)= CONVERT (VARCHAR(8), GETDATE(), 112) 
	--DECLARE @RowsUploaded INT =0
	--DECLARE @RowsFailed INT=0
	--DECLARE @RowsFailedAtTSS INT=0
	--------Generate Batch Id 
	SET @batchId = 0

	IF @BatchId = 0
	BEGIN
		DECLARE @BatchIdCal INT

		SELECT @BatchIdCal = NEXT VALUE
		FOR BatchSequenceNumber

		INSERT INTO [dbo].[BatchDetails] (
			[BatchId]
			,[Product]
			,[UploadDate]
			,[RowsUploaded]
			,[RowsFailed]
			,[RowsFailedAtTSS]
			,[MakerId]
			)
		VALUES (
			@BatchIdCal
			,-- BatchId - int
			@Product
			,-- Product - varchar(50)
			GETDATE()
			,-- UploadDate - datetime
			0
			,-- RowsUploaded - int
			0
			,-- RowsFailed - int
			0
			,-- RowsFailedAtTSS - int
			'SYS' -- MakerId - varchar(10)
			)

		SET @batchId = @BatchIdCal;
	END

	--ELSE
	--BEGIN
	--	UPDATE [dbo].[BatchDetails] 
	--	SET [RowsUploaded] = CASE WHEN @RowsUploaded <> 0 THEN @RowsUploaded ELSE RowsUploaded END,
	--		[RowsFailed] = CASE WHEN @RowsFailed <> 0 THEN @RowsFailed ELSE RowsFailed END,
	--		[RowsFailedAtTSS] = CASE WHEN @RowsFailedAtTSS <> 0 THEN @RowsFailedAtTSS ELSE RowsFailedAtTSS END
	--	WHERE [BatchId] = @BatchId AND [Product] = @Product
	--END
	-------------------------------------------------------------------------------------------------------------------------------------------
	--GoldLoan-----
	--IF @Product ='GL'
	--SELECT DISTINCT CUID COLLATE SQL_Latin1_General_CP1_CI_AS AS CUID
	--INTO #UploadedData
	--FROM (
	--	SELECT FromSourceSystemCustomerCode AS CUID -- Not sure why?
	--	FROM [FRANKLIN.iifl.in].TrackWizz.dbo.CKYCOUTWARDVIEW WITH (NOLOCK)
	--	WHERE FromSourceSystem = 'GoldLoan'
	--	UNION
	--	SELECT cuid collate Latin1_General_CI_AI AS CUID -- No need to upload for prospects that 
	--	FROM [dbo].[GL_CKYCIdGenerated] WITH (NOLOCK) -- already have CKYC No generated
	--	UNION
	--	SELECT DISTINCT SourceSystemCustomerCode AS CUID -- Upload again if rejected for some reason
	--	FROM [FRANKLIN.iifl.in].TrackWizz.dbo.[CoreCRMCustomerHistory] WITH (NOLOCK)
	--	WHERE SourceSystem = 'GoldLoan'
	--		AND RejectionCodes IS NULL
	--	) AS A;
	IF @Product = 'GL'
	BEGIN
		WITH CTE
		AS (
			SELECT CUID
				,ProspectNo
				,BusinessCode
				,ROW_NUMBER() OVER (
					PARTITION BY CUID ORDER BY CreatedOn DESC
					) AS rn
			FROM ISOM.LOANS.PrimaryApplicant PA WITH (NOLOCK)
			LEFT OUTER JOIN dbo.StagingCustom217CustomerFlat S WITH (NOLOCK) ON S.SourceSystemCustomerCode = PA.CUID
			WHERE BusinessCode = 'GL'
				AND S.SourceSystemCustomerCode IS NULL
				AND PA.LoanStatus <> 'PEN'
				AND PA.CUID IN (
					'A94DBA38' --STATUS CLS
					,'78715C39' -- ACT
					,'C13ECA48'
					--new	  '
					,'A7C669C5'
					,'ABCA8760'
					,'A1B03FF9'
					,'ABCA8760'
					,'A8406EDB'
					---2018 '
					,'A51C05BC'
					--in PA and iiflgold photographs
					,'00155E21'
					,'038C5CEA'
					)
			)
		--insertion into stagingcustom217customerflat
		INSERT INTO Clients.dbo.StagingCustom217CustomerFlat (
			ApplicationRefNumber
			,ParentCompany
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
			--,CountryOfResidence
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
			,KYCVerificationBranch
			,KYCEmployeeCode
			--,PermanentAddressProof
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
			--,ProofOfIDSubmitted
			,Minor
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
			,EmployerName
			,EmployerAddress
			,EmployerListed
			,EmployerOrganisationType
			,CurrentEmploymentInYears
			,ModuleApplicable
			,AddedBy
			,AddedOn
			,GUID
			--,CASE WHEN ISNULL(PA.PAN,'')='' THEN 1 ELSE 0 END  FormSixty
			,FormSixty
			,NPRLetter
			,KYCAttestationType
			,batchId
			,IsValid
			)
		SELECT CTE.ProspectNo AS ApplicationRefNumber
			,'IIFL12' AS ParentCompany
			,'GL' + FORMAT(GETDATE(), 'yyyyMMddHHmm') + RIGHT('0000000' + CONVERT(VARCHAR(10), DENSE_RANK() OVER (
						ORDER BY [CTE].[CUID]
						)), 10) AS TransactionID
			,'GoldLoan' AS SourceSystemName
			,CTE.CUID [SourceSystemCustomerCode]
			,CASE 
				WHEN ISDATE([PA].[CreatedOn]) = 1
					THEN FORMAT([PA].[CreatedOn], 'dd-MMM-yyyy')
				ELSE NULL
				END AS [SourceSystemCustomerCreationDate]
			,0 AS [IsSmallCustomer]
			,0 AS EkycOTPbased
			,'' AS RecordIdentifier
			,'' AS Segments
			,'' AS SegmentStartDate
			,'' AS ProductSegments
			,CASE 
				WHEN LoanStatus = 'ACT'
					THEN 'Active'
				WHEN LoanStatus = 'CLS'
					THEN 'Closed'
				ELSE 'Suspended'
				END AS CustomerStatus
			,FORMAT(CONVERT(DATE, [PA].CreatedOn), 'dd-MMM-yyyy') AS CustomerStatusEffectiveDate
			,'' AS RelatedPartyStatus
			,'' AS RelatedPartyStatusEffectiveDate
			,'1' AS [CustomerType]
			,'' AS CustomerSubType
			,PA.Title AS Prefix
			,dbo.RemoveNonAlphaCharacters(PA.FirstName)
			,PA.MiddleName
			,PA.LastName
			,'' AS MaidenPrefix
			,'' AS MaidenFirstName
			,'' AS MaidenMiddleName
			,'' AS MaidenLastName
			,CASE 
				WHEN dbo.RemoveNonAlphaCharacters(ISNULL(FI.[FatherFirstName], '')) <> ''
					THEN 'Mr'
				ELSE ''
				END AS [FatherPrefix]
			,dbo.RemoveNonAlphaCharacters(FI.FatherFirstName)
			,FI.FatherMiddleName
			,FI.FatherLastName
			,FI.SpouseTitle AS SpousePrefix
			,dbo.RemoveNonAlphaCharacters(FI.SpouseFirstName)
			,FI.SpouseMiddleName
			,FI.SpouseLastName
			,CASE 
				WHEN dbo.RemoveNonAlphaCharacters(ISNULL(FI.MotherFirstName, '')) <> ''
					THEN 'Mrs'
				ELSE ''
				END AS [MotherPrefix]
			,dbo.RemoveNonAlphaCharacters(FI.MotherFirstName) AS MotherFirstName
			,FI.MotherMiddleName AS MotherMiddleName
			,FI.MotherLastName AS MotherLastName
			,PA.Gender AS Gender
			,FI.MaritalStatus AS [MaritalStatus]
			,'IN' AS Citizenship
			--,AD.Country AS [CountryOfResidence]
			,'' AS OccupationType
			,'' AS ActivitySector
			,'' AS NatureOfBusiness
			,'' AS NatureOfBusinessOther
			,PA.DateofBirth
			,'' AS WorkEmail
			,PA.EmailAddress PersonalEmail
			,PA.CreatedOn KYCDateofDeclaration
			,PA.SubLocation KYCPlaceofDeclaration
			,PA.CreatedOn KYCVerificationDate
			,'HO' AS KYCVerificationBranch
			,PA.ClosureMakerId KYCEmployeeCode
			--,CASE 
			--	WHEN ISNULL(PA.AadhaarNumber, '') <> ''
			--		THEN 'AadharCard'
			--	WHEN ISNULL(PA.PassportNumber, '') <> ''
			--		THEN 'Passport'
			--	WHEN ISNULL(PA.VoterId, '') <> ''
			--		THEN 'VoterID'
			--	WHEN ISNULL(PA.DrivingLicenseNumber, '') <> ''
			--		THEN 'DrivingLicence'
			--	WHEN ISNULL(PA.PAN, '') <> ''
			--		THEN 'PAN'
			--	ELSE 'AadharCard'
			--	END [PermanentAddressProof]
			,'' DeskPersonalISDCode
			,NULL AS DeskPersonalSTDCode
			,NULL AS DeskPersonalTelephoneNumber
			,'' DeskWorkISDCode
			,NULL AS DeskWorkSTDCode
			,NULL AS DeskWorkTelephoneNumber
			,'' WorkMobileISD
			,'' WorkMobileNumber
			,NULL AS PersonalMobileISD
			,PA.PrimaryMobileNumber AS PersonalMobileNumber
			,NULL AS CKYCID
			,CASE 
				WHEN ISNULL(PA.PassportNumber, '') != ''
					THEN 'IN'
				ELSE ''
				END AS PassportIssueCountry
			,PA.PassportNumber AS PassportNumber
			,CAST(PA.PassportNumberExpiry AS VARCHAR) PassportExpiryDate
			,PA.VoterId AS VoterIdCard
			,PA.PAN AS PAN
			,PA.DrivingLicenseNumber AS DrivingLicenseNumber
			,CAST(PA.DrivingLicenseExpiry AS VARCHAR) DrivingLicenseExpirydate
			,PA.AadhaarNumber AS Aadhaar
			,'' AadhaarVaultReferenceNumber
			,'' AadhaarToken
			,'' AadhaarVirtualId
			,'' NREGA
			,'' CKYCPOIOtherCentralGovtID
			,'' CKYCPOIS01IDNumber
			,'' CKYCPOIS02IDNumber
			,'' NationalID
			,'' TaxIdLocal
			,'' CompanyRegistrationNumber
			,'' CompanyRegistrationCountry
			,'' GIIN
			,'' OthersInd
			,'' OthersNonInd
			--,CASE 
			--	WHEN ISNULL(PA.AadhaarNumber, '') <> ''
			--		THEN 'AadharCard'
			--	WHEN ISNULL(PA.PassportNumber, '') <> ''
			--		THEN 'Passport'
			--	WHEN ISNULL(PA.VoterId, '') <> ''
			--		THEN 'VoterID'
			--	WHEN ISNULL(PA.DrivingLicenseNumber, '') <> ''
			--		THEN 'DrivingLicence'
			--	WHEN ISNULL(PA.PAN, '') <> ''
			--		THEN 'PAN'
			--	ELSE 'AadharCard'
			--	END ProofOfIDSubmitted
			,0 AS Minor
			,'' HolderforImages
			,LI.Branch AS IntermediaryCode
			,NULL Listed
			,NULL Industry
			,NULL Nationality
			,NULL CountryofOperation
			,NULL RegulatoryAMLRisk
			,NULL RegAMLSpecialCategory
			,NULL RegAMLSpecialCategoryStartDate
			,NULL RegAMLSpecialCategoryEndDate
			,NULL LastRiskReviewDate
			,NULL NextRiskReviewDate
			,NULL IncomeRange
			,NULL ExactIncome
			,NULL IncomeCurrency
			,NULL IncomeEffectiveDate
			,NULL IncomeDescription
			,NULL IncomeProof
			,NULL ExactNetworth
			,NULL NetworthCurrency
			,NULL NetworthEffectiveDate
			,NULL NetworthDescription
			,NULL NetworthProof
			,NULL PEP
			,NULL PEPClassification
			,NULL AdverseMedia
			,NULL AdverseMediaClassification
			,NULL AdverseMediaDetails
			,NULL InsiderInformation
			,NULL Tags
			,NULL FamilyCode
			,NULL Channel
			,NULL Links
			,NULL ReputationClassification
			,NULL IUPartyType
			,NULL PropertyOwnerFlag
			,NULL ContactPersonFirstName
			,NULL ContactPersonMiddleName
			,NULL ContactPersonLastName
			,NULL ContactPersonDesignation
			,NULL ContactPersonMobileISD
			,NULL ContactPersonMobileNo
			,NULL ContactPersonMobileISD2
			,NULL ContactPersonMobileNo2
			,NULL ContactPersonEmailId1
			,NULL ContactPersonEmailId2
			,NULL RMUserCode
			,NULL RMType
			,NULL RMFromDate
			,NULL EducationalQualification
			,NULL EmployerName
			,NULL EmployerAddress
			,NULL EmployerListed
			,NULL EmployerOrganisationType
			,NULL CurrentEmploymentInYears
			,'CKYC' ModuleApplicable
			,PA.CreatedBy AS AddedBy
			,GETDATE() AS AddedOn
			,'' GUID
			,PA.HasForm60 AS FormSixty
			,'' NPRLetter
			,'01' KYCAttestationType
			,@batchid BatchId
			,'Y' IsValid
		FROM ISOM.LOANS.PrimaryApplicant PA WITH (NOLOCK)
		JOIN ISOM.LOANS.FamilyInformation FI WITH (NOLOCK) ON PA.ProspectNo = FI.ProspectNo
			AND PA.BusinessCode = FI.BusinessCode
		JOIN ISOM.LOANS.LoanInfo LI WITH (NOLOCK) ON FI.ProspectNo = LI.ProspectNo
			AND FI.BusinessCode = LI.BusinessCode
		JOIN CTE ON CTE.ProspectNo = PA.ProspectNo
			AND CTE.BusinessCode = PA.BusinessCode
		WHERE PA.BusinessCode = 'GL'
			AND CTE.rn = 1;

		------Update addresses----
		---update current address to permanent addresses------ 
		UPDATE A
		SET A.PermanentCKYCAddressType = LA.AddressType
			,A.PlotnoSurveynoHouseFlatno = ''
			,A.PermanentAddressCountry = 'IN'
			,A.PermanentAddressPinCode = LA.Pincode
			,A.PermanentAddressLine1 = LA.AddressLine1
			,A.PermanentAddressLine2 = LA.AddressLine2
			,A.PermanentAddressLine3 = LA.AddressLine3
			,A.PermanentAddressDistrict = ''
			,A.PermanentAddressCity = VA.City
			,A.PermanentAddressState = LA.STATE
		FROM dbo.StagingCustom217CustomerFlat A
		INNER JOIN ISOM.Loans.Addresses LA ON A.ApplicationRefNumber = LA.ProspectNo
			AND LA.BusinessCode = 'GL'
			AND LA.AddressType = 'CURRENT ADDRESS AS PER KYC'
		INNER JOIN dbo.view_Addresses_refer VA ON A.ApplicationRefNumber = VA.ad_ProspectNo
		WHERE A.BatchId = @batchid
			AND A.ApplicationRefNumber = LA.ProspectNo
			AND LEFT(A.TransactionID, 2) = 'GL';
		
		---update contact details address to correspondence addresses------ 
		UPDATE B
		SET B.CorrespondenceAddressCountry = 'IN'
			,B.CorrespondenceAddressPinCode = LA.Pincode
			,B.CorrespondenceAddressLine1 = LA.AddressLine1
			,B.CorrespondenceAddressLine2 = LA.AddressLine2
			,B.CorrespondenceAddressLine3 = LA.AddressLine3
			,B.CorrespondenceAddressDistrict = ''
			,B.CorrespondenceAddressCity = VA.City
			,B.CorrespondenceAddressState = LA.STATE
		FROM dbo.StagingCustom217CustomerFlat B
		INNER JOIN ISOM.Loans.Addresses LA WITH (NOLOCK) ON B.ApplicationRefNumber = LA.ProspectNo
			AND LA.BusinessCode = 'GL'
			AND LA.AddressType = 'Contact Details'
		INNER JOIN dbo.view_Addresses_refer VA ON B.ApplicationRefNumber = VA.ad_ProspectNo
		WHERE B.BatchId = @batchid
			AND B.ApplicationRefNumber = LA.ProspectNo
			AND LEFT(B.TransactionID, 2) = 'GL';
		
		---update contact details to permanent when permanent address is not updated before--
		UPDATE A
		SET A.PermanentCKYCAddressType = LA.AddressType
			,A.PlotnoSurveynoHouseFlatno = ''
			,A.PermanentAddressCountry = 'IN'
			,A.PermanentAddressPinCode = LA.Pincode
			,A.PermanentAddressLine1 = LA.AddressLine1
			,A.PermanentAddressLine2 = LA.AddressLine2
			,A.PermanentAddressLine3 = LA.AddressLine3
			,A.PermanentAddressDistrict = ''
			,A.PermanentAddressCity = VA.City
			,A.PermanentAddressState = LA.STATE
		FROM dbo.StagingCustom217CustomerFlat A
		INNER JOIN ISOM.Loans.Addresses LA ON A.ApplicationRefNumber = LA.ProspectNo
			AND LA.BusinessCode = 'GL'
			AND LA.AddressType = 'Contact Details'
		INNER JOIN dbo.view_Addresses_refer VA ON A.ApplicationRefNumber = VA.ad_ProspectNo
		WHERE A.BatchId = @batchid
			AND A.ApplicationRefNumber = LA.ProspectNo
			AND LEFT(A.TransactionID, 2) = 'GL'
			AND A.PermanentAddressLine1 IS NULL;

		---update current address details to correspondence when correspondence address is not updated before--
		UPDATE B
		SET B.CorrespondenceAddressCountry = 'IN'
			,B.CorrespondenceAddressPinCode = LA.Pincode
			,B.CorrespondenceAddressLine1 = LA.AddressLine1
			,B.CorrespondenceAddressLine2 = LA.AddressLine2
			,B.CorrespondenceAddressLine3 = LA.AddressLine3
			,B.CorrespondenceAddressDistrict = ''
			,B.CorrespondenceAddressCity = VA.City
			,B.CorrespondenceAddressState = LA.STATE
		FROM dbo.StagingCustom217CustomerFlat B
		INNER JOIN ISOM.Loans.Addresses LA ON B.ApplicationRefNumber = LA.ProspectNo
			AND LA.BusinessCode = 'GL'
			AND AddressType = 'CURRENT ADDRESS AS PER KYC'
		INNER JOIN dbo.view_Addresses_refer VA ON B.ApplicationRefNumber = VA.ad_ProspectNo
		WHERE B.BatchId = @batchid
			AND B.ApplicationRefNumber = LA.ProspectNo
			AND LEFT(B.TransactionID, 2) = 'GL'
			AND B.CorrespondenceAddressLine1 IS NULL;

		--DELETE
		--FROM Clients.dbo.StagingCustom217CustomerFlat
		--WHERE BatchId = @batchId
		--	AND SourceSystemName = 'Goldloan'
		--	AND SourceSystemCustomerCode IN (
		--		SELECT DISTINCT cuid
		--		FROM #UploadedData
		--		)
		--	AND ApplicationRefNumber IN
		--	(
		--			'GL1575'
		--			,'GL11130'
		--			,'GL10399'
		--			,'GL2535'
		--			,'GL25355'
		--			,'GL100283'
		--			,'GL100301'
		--			,'GL100302'
		--			);
		---------------Start for image processing 
		WITH CTE
		AS (
			SELECT ROW_NUMBER() OVER (
					PARTITION BY tkd.CUID
					,tkd.FamilyId ORDER BY tkd.srno DESC
					) AS rn
				,tkd.CUID
				,CASE 
					WHEN tkd.FamilyID = 7
						AND tdm.Srno = 17
						THEN 'passport' --and FamilyDescription ='Passport' 
					WHEN tkd.FamilyID = 7
						AND tdm.Srno = 53
						THEN 'aadharcard' -- and FamilyDescription ='Aadhaar Card'
					WHEN tkd.FamilyID = 7
						AND tdm.Srno = 54
						THEN 'drivinglicense' --and FamilyDescription ='Driving License'
					WHEN tkd.FamilyID = 7
						AND tdm.Srno = 55
						THEN 'voter' --and FamilyDescription ='Election ID Card'
					WHEN tkd.FamilyID = 8
						AND tdm.Srno = 56
						AND FamilyDescription = 'Aadhaar Card'
						THEN 'aadharcard'
					WHEN tkd.FamilyID = 8
						AND tdm.Srno = 22
						AND FamilyDescription = 'Passport'
						THEN 'passport'
					WHEN tkd.FamilyID = 8
						AND tdm.Srno = 23
						AND FamilyDescription = 'Driving License'
						THEN 'drivinglicense'
					WHEN tkd.FamilyID = 8
						AND tdm.Srno = 24
						AND FamilyDescription = 'Election ID Card'
						THEN 'voter'
					WHEN tkd.FamilyID = 8
						AND tdm.Srno = 25
						AND FamilyDescription = 'PAN Card'
						THEN 'pan'
					ELSE ''
					END AS AttachmentCode
			FROM IIFLGOLD.dbo.tbl_KYCdoc_Details tkd WITH (NOLOCK)
			INNER JOIN IIFLGOLD.dbo.Tbl_DocumentMaster tdm WITH (NOLOCK) ON tdm.Srno = tkd.DocumentID
			WHERE (
					tkd.FamilyID IN (
						7
						,8
						)
					AND tkd.IsKYCDoc = 'Y'
					)
				AND tkd.DocumentID > 0
				AND tkd.image_path IS NOT NULL
			)

		--update proof fields ---
		UPDATE Cus
		SET CUS.PermanentAddressProof = CTE.AttachmentCode
			,ProofOfIDSubmitted = CTE.AttachmentCode
			,CUS.CorrespondenceAddressProof = CTE.AttachmentCode
		FROM Clients.dbo.StagingCustom217CustomerFlat Cus
		INNER JOIN CTE ON CTE.CUID = Cus.SourceSystemCustomerCode
		WHERE BatchId = @BatchId
			AND Cus.SourceSystemName = 'Goldloan'
			AND CUS.IsValid = 'Y'
			AND ISNULL(CTE.AttachmentCode, '') <> ''
			AND CTE.AttachmentCode <> 'pan'
			AND CTE.rn = 1;

		--Update IsValid='N' if no correspondenceaddressproof
		UPDATE A
		SET A.IsValid = 'N'
			,A.InvalidColRemarks = 'CorrespondenceAddressProof address images not found'
		FROM Clients.dbo.StagingCustom217CustomerFlat A WITH (NOLOCK)
		WHERE SourceSystemName = 'Goldloan'
			AND batchId = @BatchId
			AND IsValid = 'Y'
			AND ISNULL(CorrespondenceAddressProof, '') = ''

		--Update IsValid='N' for invalid PermanentAddressProof
		UPDATE B
		SET B.IsValid = 'N'
			,B.InvalidColRemarks = CASE 
				WHEN (
						ISNULL(PermanentAddressProof, '') = ''
						OR ISNULL(PermanentAddressProof, '') LIKE '%PAN%'
						)
					THEN 'Address proof is required.'
				WHEN PermanentAddressProof LIKE '%Aadhar%'
					AND ISNULL(Aadhaar, '') = ''
					THEN 'Aadhar number is not present for uploaded image.'
				WHEN PermanentAddressProof LIKE '%PAN%'
					THEN 'Pan is invalid address proof'
				WHEN PermanentAddressProof LIKE '%Passport%'
					AND ISNULL(PassportNumber, '') = ''
					THEN 'Passport number not provided for uploaded image'
				WHEN PermanentAddressProof LIKE '%VoterID%'
					AND ISNULL(VoterIdCard, '') = ''
					THEN 'Voter number not provided for uploaded image'
				WHEN PermanentAddressProof LIKE '%Driving%'
					AND ISNULL(DrivingLicenseNumber, '') = ''
					THEN 'Driving Licence number not provided for uploaded image'
				ELSE ''
				END
		FROM Clients.dbo.StagingCustom217CustomerFlat B WITH (NOLOCK)
		WHERE SourceSystemName = 'Goldloan'
			AND batchId = @BatchId
			AND CustomerIntegrationStatus IS NULL
			AND IsValid = 'Y'
			AND (
				(
					ISNULL(PermanentAddressProof, '') = ''
					OR ISNULL(PermanentAddressProof, '') LIKE '%PAN%'
					)
				OR (
					PermanentAddressProof LIKE '%Aadhar%'
					AND ISNULL(Aadhaar, '') = ''
					)
				OR (
					PermanentAddressProof LIKE '%PAN%'
					AND ISNULL(PAN, '') = ''
					)
				OR (
					PermanentAddressProof LIKE '%Passport%'
					AND ISNULL(PassportNumber, '') = ''
					)
				OR (
					PermanentAddressProof LIKE '%VoterID%'
					AND ISNULL(VoterIdCard, '') = ''
					)
				OR (
					PermanentAddressProof LIKE '%Driving%'
					AND ISNULL(DrivingLicenseNumber, '') = ''
					)
				);

		--DELETE
		--FROM #CustomerImages
		--WHERE CUID IN (
		--		SELECT SourceSystemCustomerCode
		--		FROM #tempInValidRecord
		--		)
		--AND ProspectNo IN (
		--			'GL1575'
		--			,'GL11130'
		--			,'GL10399'
		--			,'GL2535'
		--			,'GL25355'
		--			,'GL100283'
		--			,'GL100301'
		--			,'GL100302'
		--			);
		--------------------End here Invalid permanant address proof not found 
		WITH CTE_image
		AS (
			SELECT ROW_NUMBER() OVER (
					PARTITION BY tkd.CUID
					,tkd.FamilyId ORDER BY tkd.srno DESC
					) AS rn
				,tkd.CUID AS CUID
				,CASE 
					WHEN tkd.FamilyID = 7
						AND tdm.Srno = 17
						THEN 'passport' --and FamilyDescription ='Passport' 
					WHEN tkd.FamilyID = 7
						AND tdm.Srno = 53
						THEN 'aadharcard' -- and FamilyDescription ='Aadhaar Card'
					WHEN tkd.FamilyID = 7
						AND tdm.Srno = 54
						THEN 'drivinglicense' --and FamilyDescription ='Driving License'
					WHEN tkd.FamilyID = 7
						AND tdm.Srno = 55
						THEN 'voter' --and FamilyDescription ='Election ID Card'
					WHEN tkd.FamilyID = 8
						AND tdm.Srno = 56
						AND FamilyDescription = 'Aadhaar Card'
						THEN 'aadharcard'
					WHEN tkd.FamilyID = 8
						AND tdm.Srno = 22
						AND FamilyDescription = 'Passport'
						THEN 'passport'
					WHEN tkd.FamilyID = 8
						AND tdm.Srno = 23
						AND FamilyDescription = 'Driving License'
						THEN 'drivinglicense'
					WHEN tkd.FamilyID = 8
						AND tdm.Srno = 24
						AND FamilyDescription = 'Election ID Card'
						THEN 'voter'
					WHEN tkd.FamilyID = 8
						AND tdm.Srno = 25
						AND FamilyDescription = 'PAN Card'
						THEN 'pan'
					ELSE ''
					END AS AttachmentCode
				,tkd.Srno AS TransactionID
				,CASE 
					WHEN tkd.image_path LIKE '%\%'
						THEN RIGHT(tkd.image_path, CHARINDEX('\', REVERSE(tkd.image_path), - 1) - 1)
					ELSE ''
					END AS FileName
				,tkd.image_path AS FilePath
				,CASE 
					WHEN tkd.image_path LIKE '%.%'
						THEN REVERSE(SUBSTRING(REVERSE(tkd.image_path), 1, CHARINDEX('.', REVERSE(tkd.image_path)) - 1))
					END FileType
				,tkd.mkrid AS mkrid
			FROM IIFLGOLD.dbo.tbl_KYCdoc_Details tkd WITH (NOLOCK)
			INNER JOIN IIFLGOLD.dbo.Tbl_DocumentMaster tdm WITH (NOLOCK) ON tdm.Srno = tkd.DocumentID
			WHERE (
					tkd.FamilyID IN (
						7
						,8
						)
					AND tkd.IsKYCDoc = 'Y'
					)
				AND tkd.DocumentID > 0
				AND tkd.image_path IS NOT NULL
			)

		---insert document image path in customerimagesall---
		INSERT INTO CustomerImagesAll (
			BusinessCode
			,TransactionId
			,FileName
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
		SELECT 'GL' AS BusinessCode
			,CTE_image.TransactionID
			,CTE_image.FileName
			,CTE_image.FilePath
			,CTE_image.FileType
			,CTE_image.AttachmentCode AS DocumentType
			,s217.ApplicationRefNumber AS AppRefNoForImages
			,CTE_image.CUID
			,'N' AS BinaryFormat
			,'GL' AS Product
			,'N' AS IsUploaded
			,CTE_image.mkrid
			,GETDATE() AS AddedOn
			,CONVERT(VARCHAR(100), @BatchId) AS Batchid
		FROM dbo.StagingCustom217CustomerFlat s217 WITH (NOLOCK)
		INNER JOIN CTE_image ON CTE_image.CUID = s217.SourceSystemCustomerCode
		WHERE s217.IsValid = 'Y'
			AND s217.BatchId = @BatchId
			AND CTE_image.AttachmentCode != ''
			AND CTE_image.rn = 1;


		;WITH CTE_photograph as

		(
		SELECT ROW_NUMBER() OVER ( ORDER BY s217.ApplicationRefNumber DESC) AS rn

		FROM IIFLGOLD.dbo.tbl_IPPhotographs A WITH (NOLOCK)
			 INNER JOIN dbo.StagingCustom217CustomerFlat s217 WITH(NOLOCK)
				 ON A.ProspectNo=s217.ApplicationRefNumber
		)
		---insert client image path in customerimagesall---
		INSERT INTO CustomerImagesAll (
			BusinessCode
			,TransactionId
			,FileName
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
		SELECT 'GL'
			,CONVERT(VARCHAR(100), A.Srno) AS TransactionID
			,CASE 
				WHEN A.ClientImagePath IS NOT NULL
					THEN RIGHT(A.ClientImagePath, CHARINDEX('\', REVERSE(A.ClientImagePath), 1) - 1)
				ELSE 'Photo_' + ISNULL(A.ProspectNo, '') + '.jpg'
				END AS FileName
			,CASE 
				WHEN A.ClientImagePath IS NOT NULL
					THEN A.ClientImagePath
				ELSE '\\AZTRUELIES2\CKYC Images\' + s217.SourceSystemCustomerCode  + '\Photo_' + ISNULL(A.ProspectNo, '') + '.jpg'
				END AS FilePath
			,CASE 
				WHEN A.ClientImagePath IS NOT NULL
					THEN LOWER('.' + RIGHT(a.ClientImagePath, CHARINDEX('.', REVERSE(a.ClientImagePath)) - 1))
				ELSE '.jpg'
				END AS Filetype
			,'photograph' AS DocumentType
			,A.ProspectNo
			,s217.SourceSystemCustomerCode 
			,CASE 
				WHEN A.ClientImagePath IS NULL
					THEN 'Y'
				ELSE 'N'
				END AS BinaryFormat
			,'GL' Product
			,'N' AS IsUploaded
			,A.MakerID
			,GETDATE() AS AddedOn
			,CONVERT(VARCHAR(100), @BatchId) AS Batchid
		FROM IIFLGOLD.dbo.tbl_IPPhotographs A WITH (NOLOCK)
		INNER JOIN dbo.StagingCustom217CustomerFlat s217 WITH (NOLOCK) ON A.ProspectNo = s217.ApplicationRefNumber
		INNER JOIN CTE_photograph C  ON A.ProspectNo=s217.ApplicationRefNumber
		WHERE 
			 s217.IsValid = 'Y'
			AND s217.BatchId = @BatchId
			AND C.rn=1

		SELECT CONVERT(VARCHAR(100), a.Srno) ID
			,a.FileName AS NAME
			,a.FilePath
			,Filetype
			,DocumentType
			,AppRefNoForImages
			,Cuid
			,mkrid AS MkrID
			,BinaryFormat
			,CASE 
				WHEN a.BinaryFormat = 'Y'
					THEN ip.ClientImage
				ELSE NULL
				END AS ClientImage
			,a.Batchid AS BatchId
			,Product
		FROM CustomerImagesAll a WITH (NOLOCK)
		INNER JOIN IIFLGOLD.dbo.tbl_IPPhotographs ip WITH (NOLOCK) ON a.AppRefNoForImages = ip.ProspectNo
		WHERE Product = 'GL'
			AND IsUploaded = 'N'
			AND a.Batchid = @BatchId
		ORDER BY a.Srno ASC;
	END
END
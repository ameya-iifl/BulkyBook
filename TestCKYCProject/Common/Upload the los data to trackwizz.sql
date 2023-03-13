--WARNING! ERRORS ENCOUNTERED DURING SQL PARSING!
TEXT

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--=========================================================================
---Created By :  Ravindra Sapkal
---Created On :  14 Feb 2019
---Changed on :  28 FEb 2019
--================================================================================
/*
--------------------------------------------------------------------------------------------------------------------------
Author				Date		Remarks
--------------------------------------------------------------------------------------------------------------------------
Sameer Naik			14-07-2022	Add logs for record 
Sameer Naik			18-07-2022	Added error log.
--------------------------------------------------------------------------------------------------------------------------

--Execute Proc_Upload_PLBYJUCustomerAndImageDataOnTrackWizzServer '43'
*/
CREATE PROC [dbo].[Proc_Upload_PLBYJUCustomerAndImageDataOnTrackWizzServer] @BatchId VARCHAR(100) = '0'
AS
BEGIN
	BEGIN TRY
		IF (CAST(@BatchId AS INT) > 0)
		BEGIN
			--================Upload photograph in the stagincustom168 attachement
			INSERT INTO dbo.CKYCDataUploadLog (
				processname
				,methodname
				,activity
				,CreatedOn
				,RecordsProcessed
				,BatchId
				,Response
				)
			VALUES (
				'DF Ckyc Data Upload'
				,'Proc_Upload_PLBYJUCustomerAndImageDataOnTrackWizzServer'
				,'Upload DF data in trackwise process-Start'
				,GETDATE()
				,0
				,@BatchId
				,'Process-Start'
				)

			INSERT INTO StagingCustom168CKYCAttachment (
				TransactionId
				,ImageName
				,FilePath
				,FileExtension
				,AttachmentCode
				,AppRefnumberForImages
				,SourceSysytemCustomerCode
				,AddedBy
				,AddedOn
				,BatchID
				,Product
				,IsUploaded
				)
			SELECT TransactionId
				,ImageName
				,replace(FilePath, 'F:\CKYC Images Aadhar\', '\\AZTRUELIES2\CKYC Images\') AS FilePath
				,CASE 
					WHEN FileExtension NOT LIKE '%.%'
						THEN '.' + lower(isnull(FileExtension, ''))
					ELSE lower(FileExtension)
					END FileExtension
				,lower(AttachmentCode) AS AttachmentCode
				,AppRefnumberForImages
				,SourceSysytemCustomerCode
				,AddedBy
				,Getdate() AddedOn
				,BatchId
				,Product
				,'Y' AS IsUploaded
			FROM StagingCKYCAttachmentView
			WHERE Product = 'PLBYJU'
				AND BatchId = @BatchId

			---==============Attachment Data Upload
			INSERT INTO [Franklin.iifl.in].Trackwizz.dbo.StagingCustom168CKYCAttachment (
				TransactionId
				,ImageName
				,FilePath
				,FileExtension
				,AttachmentCode
				,AppRefnumberForImages
				,SourceSysytemCustomerCode
				,AddedBy
				,AddedOn
				)
			SELECT TransactionId
				,ImageName
				,FilePath
				,FileExtension
				,AttachmentCode
				,AppRefnumberForImages
				,SourceSysytemCustomerCode
				,AddedBy
				,AddedOn
			FROM (
				SELECT a.TransactionId
					,CASE 
						WHEN a.ImageName NOT LIKE '%.%'
							AND a.attachmentcode IN (
								'Aadhaar Card'
								,'aadhar_card'
								)
							THEN a.ImageName + '.pdf'
						ELSE a.ImageName
						END ImageName
					,CASE 
						WHEN RIGHT(a.fILEPATH, 1) = '\'
							THEN LEFT(a.FilePath, LEN(a.FILEPATH) - 1)
						ELSE a.FilePath
						END AS FilePath
					,CASE 
						WHEN a.FileExtension IS NULL
							AND a.ImageName LIKE 'AadharCard%'
							THEN '.pdf'
						WHEN a.FileExtension NOT LIKE '.%'
							THEN '.' + LOWER(a.FileExtension)
						ELSE LOWER(a.FileExtension)
						END FileExtension
					,a.AttachmentCode
					,a.AppRefnumberForImages
					,a.SourceSysytemCustomerCode
					,a.AddedBy
					,Getdate() AS AddedOn
				FROM StagingCustom168CKYCAttachment a WITH (NOLOCK)
				WHERE product = 'PLBYJU'
					AND BatchID = @BatchId
				) AS t

			INSERT INTO dbo.CKYCDataUploadLog (
				processname
				,methodname
				,activity
				,CreatedOn
				,RecordsProcessed
				,BatchId
				,Response
				)
			VALUES (
				'DF Ckyc Data Upload'
				,'Proc_Upload_PLBYJUCustomerAndImageDataOnTrackWizzServer'
				,'DF Image data upload in trackwise'
				,GETDATE()
				,@@ROWCOUNT
				,@BatchId
				,'DF Image data is uploaded in Trackwizz.dbo. StagingCustom168CKYCAttachment table.'
				)

			SELECT DISTINCT SourceSysytemCustomerCode
			INTO #CustomerUploaded
			FROM StagingCustom168CKYCAttachment WITH (NOLOCK)
			WHERE product = 'PLBYJU'
				AND BatchID = @BatchId
				AND AttachmentCode = 'photograph'

			INSERT INTO [Franklin.iifl.in].Trackwizz.dbo.StagingCustom217CustomerFlat (
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
				)
			SELECT 'IIFL 1' AS ParentCompany
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
				,RTRIM(REPLACE(Prefix, '.', '')) AS Prefix
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
				,CASE 
					WHEN ISNULL(MotherFirstName, '') != ''
						THEN 'Mrs'
					ELSE ''
					END MotherPrefix
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
				,CASE 
					WHEN ISNULL(KYCEmployeeName, '') != ''
						THEN RTRIM(LTRIM(dbo.RemoveNonAlphaCharacters(KYCEmployeeName)))
					ELSE 'Byjus'
					END AS KYCEmployeeName
				,CASE 
					WHEN ISNULL(KYCEmployeeDesignation, '') != ''
						THEN RTRIM(LTRIM(dbo.RemoveNonAlphaCharacters(KYCEmployeeDesignation)))
					ELSE 'Vendor'
					END AS KYCEmployeeDesignation
				,KYCVerificationBranch
				,CASE 
					WHEN ISNULL(KYCEmployeeCode, '') != ''
						THEN RTRIM(LTRIM(ISNULL(KYCEmployeeCode, '')))
					ELSE 'KreditBee'
					END AS KYCEmployeeCode
				,PermanentCKYCAddressType
				,PlotnoSurveynoHouseFlatno
				,PermanentAddressCountry
				,PermanentAddressPinCode
				,RTRIM(LTRIM([dbo].[RemoveRepeatingChars](IIFLGOLD.dbo.RemoveSpaceForCersaiChargeReporting(REPLACE(REPLACE(PermanentAddressLine1, ',', ' '), '.', ' '))))) PermanentAddressLine1
				,RTRIM(LTRIM([dbo].[RemoveRepeatingChars](IIFLGOLD.dbo.RemoveSpaceForCersaiChargeReporting(REPLACE(REPLACE(PermanentAddressLine2, ',', ' '), '.', ' '))))) PermanentAddressLine2
				,RTRIM(LTRIM([dbo].[RemoveRepeatingChars](IIFLGOLD.dbo.RemoveSpaceForCersaiChargeReporting(REPLACE(REPLACE(PermanentAddressLine3, ',', ' '), '.', ' '))))) PermanentAddressLine3
				,PermanentAddressDistrict
				,PermanentAddressCity
				,'' AS PermanentAddressState
				,PermanentAddressProof
				,CorrespondenceAddressCountry
				,CorrespondenceAddressPinCode
				,RTRIM(LTRIM([dbo].[RemoveRepeatingChars](IIFLGOLD.dbo.RemoveSpaceForCersaiChargeReporting(REPLACE(REPLACE(CorrespondenceAddressLine1, ',', ' '), '.', ' '))))) CorrespondenceAddressLine1
				,RTRIM(LTRIM([dbo].[RemoveRepeatingChars](IIFLGOLD.dbo.RemoveSpaceForCersaiChargeReporting(REPLACE(REPLACE(CorrespondenceAddressLine2, ',', ' '), '.', ' '))))) CorrespondenceAddressLine2
				,RTRIM(LTRIM([dbo].[RemoveRepeatingChars](IIFLGOLD.dbo.RemoveSpaceForCersaiChargeReporting(REPLACE(REPLACE(CorrespondenceAddressLine3, ',', ' '), '.', ' '))))) CorrespondenceAddressLine3
				,CorrespondenceAddressDistrict
				,CorrespondenceAddressCity
				,'' AS CorrespondenceAddressState
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
				,IIFLGOLD.dbo.RemoveSpaceForCersaiChargeReporting(BirthCity)
				,'' AS TaxResidencyCountry
				,'' AS TaxIdentificationNumber
				,'' AS TaxResidencyAddressCountry
				,'' AS TaxResidencyAddressLine1
				,'' AS TaxResidencyAddressLine2
				,'' AS TaxResidencyAddressLine3
				,'' AS TaxResidencyAddressPinCode
				,'' AS TaxResidencyAddressDistrict
				,'' AS TaxResidencyAddressCity
				,'' AS TaxResidencyAddressState
				,DeskPersonalISDCode
				,DeskPersonalSTDCode
				,DeskPersonalTelephoneNumber
				,DeskWorkISDCode
				,DeskWorkSTDCode
				,DeskWorkTelephoneNumber
				,WorkMobileISD
				,WorkMobileNumber
				,CASE 
					WHEN ISNULL(PersonalMobileNumber, '') <> ''
						THEN '91'
					ELSE ''
					END PersonalMobileISD
				,PersonalMobileNumber
				,CKYCID
				,'' PassportIssueCountry
				,PassportNumber
				,'' PassportExpiryDate
				,VoterIdCard
				,PAN
				,DrivingLicenseNumber
				,'' DrivingLicenseExpiryDate
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
				,GETDATE() AS AddedOn
				,GUID
				,CASE 
					WHEN ISNULL(PAN, '') <> ''
						THEN 0
					ELSE FormSixty
					END AS FormSixty
				,NPRLetter
				,KYCAttestationType
			FROM StagingCustom217CustomerFlat a WITH (NOLOCK)
			INNER JOIN #CustomerUploaded cu ON a.SourceSystemCustomerCode = cu.SourceSysytemCustomerCode
			WHERE a.SourceSystemName = 'PLBYJU'
				AND IsValid = 'Y'
				AND CustomerIntegrationStatus IS NULL
				AND batchId = @BatchId

			INSERT INTO dbo.CKYCDataUploadLog (
				processname
				,methodname
				,activity
				,CreatedOn
				,RecordsProcessed
				,BatchId
				,Response
				)
			VALUES (
				'DF Ckyc Data Upload'
				,'Proc_Upload_PLBYJUCustomerAndImageDataOnTrackWizzServer'
				,'DF valid data upload'
				,GETDATE()
				,@@ROWCOUNT
				,@BatchId
				,'DF Valid data is uploaded in Trackwizz.dbo.StagingCustom217CustomerFlat table.'
				)

			INSERT INTO dbo.CKYCDataUploadLog (
				processname
				,methodname
				,activity
				,CreatedOn
				,RecordsProcessed
				,BatchId
				,Response
				)
			VALUES (
				'DF Ckyc Data Upload'
				,'Proc_Upload_PLBYJUCustomerAndImageDataOnTrackWizzServer'
				,'DF Upload data in trackwise process-End'
				,GETDATE()
				,0
				,@BatchId
				,'Process-End'
				)
		END

		SELECT TOP 10 *
		FROM StagingCustom217CustomerFlat a WITH (NOLOCK)
		WHERE a.SourceSystemName = 'PLBYJU'
	END TRY

	BEGIN CATCH
		DECLARE @ID VARCHAR(15) = CAST(@BatchId AS VARCHAR)

		EXECUTE proc_SaveDBError @ID
	END CATCH
END 
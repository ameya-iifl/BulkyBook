CREATE PROCEDURE dbo.proc_Upload_CustomerAndImageDataOnTrackWizzServer @Product, @BatchId INT = 0
AS
BEGIN
	BEGIN TRY
		IF @BatchId > 0
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
				'DF Ckyc Data Upload' -- Replace with 'CKYC Data Upload'
				,'Proc_Upload_PLBYJUCustomerAndImageDataOnTrackWizzServer' -- Replace with 'proc_Upload_CustomerAndImageDataOnTrackWizzServer'
				,'Upload DF data in trackwise process-Start' -- Replace with 'Upload data into Trackwizz process-Start'
				,GETDATE()
				,0
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
				,replace(FilePath, 'F:\CKYC Images Aadhar\', '\\AZTRUELIES2\CKYC Images\') AS FilePath -- Check Once
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
			WHERE BatchId = @BatchId

			---==============Attachment Data Upload
			INSERT INTO [AZUATAMLCKYC\NBFC].smalloffice.dbo.StagingCustom168CKYCAttachment (
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
			SELECT 
				TransactionId
				,CASE 
					WHEN ImageName NOT LIKE '%.%'
						AND attachmentcode IN (
							'Aadhaar Card'
							,'aadhar_card'
							)
						THEN ImageName + '.pdf'
					ELSE ImageName
					END ImageName
				,CASE 
					WHEN RIGHT(fILEPATH, 1) = '\'
						THEN LEFT(FilePath, LEN(FILEPATH) - 1)
					ELSE FilePath
					END AS FilePath
				,CASE 
					WHEN FileExtension IS NULL
						AND ImageName LIKE 'AadharCard%'
						THEN '.pdf'
					WHEN FileExtension NOT LIKE '.%'
						THEN '.' + LOWER(FileExtension)
					ELSE LOWER(FileExtension)
					END FileExtension
				,AttachmentCode
				,AppRefnumberForImages
				,SourceSysytemCustomerCode
				,AddedBy
				,Getdate() AS AddedOn
			FROM StagingCustom168CKYCAttachment WITH (NOLOCK)
			WHERE BatchID = @BatchId

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
				'DF Ckyc Data Upload'  -- Replace with 'CKYC Data Upload'
				,'Proc_Upload_PLBYJUCustomerAndImageDataOnTrackWizzServer' -- Replace with 'proc_Upload_CustomerAndImageDataOnTrackWizzServer'
				,'DF Image data upload in trackwise' -- Replace with 'Image data upload into Trackwizz '
				,GETDATE()
				,@@ROWCOUNT
				,@BatchId
				,'DF Image data is uploaded in Trackwizz.dbo. StagingCustom168CKYCAttachment table.' -- replace with 'Image data is uploaded into smalloffice.dbo.StagingCustom168CKYCAttachment table.'
				)

				INSERT INTO [AZUATAMLCKYC\NBFC].smalloffice.dbo.StagingCustom217CustomerFlat (
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
			SELECT 
				ParentCompany --check
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
				,CASE 
					WHEN ISNULL(KYCEmployeeName, '') != ''
						THEN RTRIM(LTRIM(dbo.RemoveNonAlphaCharacters(KYCEmployeeName)))
					ELSE 'Byjus'
					END AS KYCEmployeeName -- Confirm as not getting inserted to begin with
				,CASE 
					WHEN ISNULL(KYCEmployeeDesignation, '') != ''
						THEN RTRIM(LTRIM(dbo.RemoveNonAlphaCharacters(KYCEmployeeDesignation)))
					ELSE 'Vendor'
					END AS KYCEmployeeDesignation -- Confirm as not getting inserted to begin with
				,KYCVerificationBranch
				,CASE 
					WHEN ISNULL(KYCEmployeeCode, '') != ''
						THEN RTRIM(LTRIM(ISNULL(KYCEmployeeCode, '')))
					ELSE 'KreditBee' 
					END AS KYCEmployeeCode -- Check
				,PermanentCKYCAddressType
				,PlotnoSurveynoHouseFlatno
				,PermanentAddressCountry
				,PermanentAddressPinCode
				,RTRIM(LTRIM([dbo].[RemoveRepeatingChars](IIFLGOLD.dbo.RemoveSpaceForCersaiChargeReporting(REPLACE(REPLACE(PermanentAddressLine1, ',', ' '), '.', ' '))))) PermanentAddressLine1  -- Check for function in clients
				,RTRIM(LTRIM([dbo].[RemoveRepeatingChars](IIFLGOLD.dbo.RemoveSpaceForCersaiChargeReporting(REPLACE(REPLACE(PermanentAddressLine2, ',', ' '), '.', ' '))))) PermanentAddressLine2  -- Check for function in clients
				,RTRIM(LTRIM([dbo].[RemoveRepeatingChars](IIFLGOLD.dbo.RemoveSpaceForCersaiChargeReporting(REPLACE(REPLACE(PermanentAddressLine3, ',', ' '), '.', ' '))))) PermanentAddressLine3  -- Check for function in clients
				,PermanentAddressDistrict
				,PermanentAddressCity
				,PermanentAddressState
				,PermanentAddressProof
				,CorrespondenceAddressCountry
				,CorrespondenceAddressPinCode
				,RTRIM(LTRIM([dbo].[RemoveRepeatingChars](IIFLGOLD.dbo.RemoveSpaceForCersaiChargeReporting(REPLACE(REPLACE(CorrespondenceAddressLine1, ',', ' '), '.', ' '))))) CorrespondenceAddressLine1 -- Check for function in clients
				,RTRIM(LTRIM([dbo].[RemoveRepeatingChars](IIFLGOLD.dbo.RemoveSpaceForCersaiChargeReporting(REPLACE(REPLACE(CorrespondenceAddressLine2, ',', ' '), '.', ' '))))) CorrespondenceAddressLine2 -- Check for function in clients
				,RTRIM(LTRIM([dbo].[RemoveRepeatingChars](IIFLGOLD.dbo.RemoveSpaceForCersaiChargeReporting(REPLACE(REPLACE(CorrespondenceAddressLine3, ',', ' '), '.', ' '))))) CorrespondenceAddressLine3 -- Check for function in clients
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
				,IIFLGOLD.dbo.RemoveSpaceForCersaiChargeReporting(BirthCity) -- Check for function in clients// inserted as blank
				,TaxIdentificationNumber
				,TaxResidencyCountry
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
				,CASE 
					WHEN ISNULL(PersonalMobileNumber, '') <> ''
						THEN '91'
					ELSE ''
					END PersonalMobileISD -- inserted as NULL
				,PersonalMobileNumber
				,CKYCID AS CKYCID
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
				,GETDATE() AS AddedOn -- Another getdate necessary??
				,GUID
				,CASE 
					WHEN ISNULL(PAN, '') <> ''
						THEN 0
					ELSE FormSixty
					END AS FormSixty -- Confirm in ISOM
				,NPRLetter
				,KYCAttestationType
			FROM StagingCustom217CustomerFlat a WITH (NOLOCK)
			INNER JOIN StagingCustom168CKYCAttachment cu WITH (NOLOCK) ON a.SourceSystemCustomerCode = cu.SourceSysytemCustomerCode
			WHERE --a.SourceSystemName = 'PLBYJU' AND 
				IsValid = 'Y'
				AND a.CustomerIntegrationStatus IS NULL
				AND a.batchId = @BatchId
				AND LEFT(a.TransactionID, 2) = @Product
				AND b.AttachmentCode = 'photograph' 


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
				'Ckyc Data Upload'
				,'Proc_Upload_CustomerAndImageDataOnTrackWizzServer'
				,'valid data upload'
				,GETDATE()
				,@@ROWCOUNT
				,@BatchId
				,'Valid data is uploaded in smalloffice.dbo.StagingCustom217CustomerFlat table.'
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
				'Ckyc Data Upload'
				,'Proc_Upload_CustomerAndImageDataOnTrackWizzServer'
				,'Upload data in trackwise process-End'
				,GETDATE()
				,0
				,@BatchId
				,'Process-End'
				)

		END

		SELECT TOP 10 *  -- Why top 10??
		FROM StagingCustom217CustomerFlat a WITH (NOLOCK)
		--WHERE a.SourceSystemName = 'PLBYJU'

	END TRY
	BEGIN CATCH
		DECLARE @ID VARCHAR(15) = CAST(@BatchId AS VARCHAR)

		EXECUTE proc_SaveDBError @ID
	END CATCH
END
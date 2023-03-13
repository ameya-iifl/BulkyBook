--Track wizz data uploaded Image :
select top 1 * from [Franklin.iifl.in].Trackwizz.dbo.StagingCustom168CKYCAttachment

--Track wizz data uploaded :
 select top 1 * FROM [Franklin.iifl.in].Trackwizz.dbo.StagingCustom217CustomerFlat a with(Nolock)


--Find Rejection Reason : 
SELECT top 10 * FROM  [Franklin.iifl.in].TrackWizz.dbo.[CoreCRMCustomerHistory]  a with(nolock)
WHERE rejectioncodes is null


--Find CKYC NUMBER : 
SELECT FromSourceSystemCustomerCode AS CUID, CKYCNUMBER
FROM [Franklin.iifl.in].TrackWizz.dbo.CKYCOUTWARDVIEW WITH(NOLOCK)
WHERE FromSourceSystem = 'PLBYJU'

select *
from Clients.[dbo].[StagingCustom217CustomerFlat]
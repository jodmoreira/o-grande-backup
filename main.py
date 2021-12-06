import aws_tools.s3_tools as s3


files = s3.list_files(
    "ogb_lake",
    "s3://ogb-lake/social_media/twitter/landing_zone/year=2021/month=10/day=10/AfonsoFlorence/",
    10,
)
print(files)

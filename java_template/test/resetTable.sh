switchBoardFunction="switchBoard"
bucketName="tcss462salesdata"

fileName="100SalesRecords.csv"
jsonT={"\"bucketname\"":\"${bucketName}\"","\"filename\"":\"${fileName}\"",""\"operationtype\"":7"}
# echo $jsonT


echo "Reset DB"
time output=`aws lambda invoke --invocation-type RequestResponse --function-name $switchBoardFunction --region us-east-2 --payload $jsonT --cli-connect-timeout 900 --cli-read-timeout 900 /dev/stdout | head -n 1 | head -c -2 ; echo`
echo ""
echo "RESULT:"
echo $output | jq
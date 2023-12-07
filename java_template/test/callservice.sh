#!/bin/bash
bucketName="test.project.tcss462.ngt"
functionName="projectTestProcessCSV"

echo "Which file would you like to use?"
echo "1. 100"
echo "2. 1000"
echo "3. 10000"
echo "4. 50000"
echo "5. 100000"
echo "6. 500000"
echo "7. 1000000"
echo "8. 1500000"

read -p "Enter your option " option
echo You entered $option

case $option in
    1) 
    option=100
    ;;

    2) 
    option=1000
    ;;

    3) 
    option=10000
    ;;

    4) 
    option=50000
    ;;

    5) 
    option=100000
    ;;

    6) 
    option=500000
    ;;

    7) 
    option=1000000
    ;;

    8) 
    option=1500000
    ;;

    *)
    echo "Entered option is invalid, we will use the default csv file with 100 records"
    option=100
    ;;

esac


# JSON object to pass to Lambda Function
fileName=${option}"SalesRecords.csv"
json={"\"row\"":50,"\"col\"":10,"\"bucketname\"":\"${bucketName}\"","\"filename\"":\"${fileName}\""}
echo $json


echo "Invoking Process CSV Lambda function using AWS CLI"
time output=`aws lambda invoke --invocation-type RequestResponse --function-name projectTestProcessCSV --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
echo ""
echo "JSON RESULT:"
echo $output | jq

#process Function procecssCSVTut5
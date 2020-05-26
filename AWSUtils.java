package com.lokesh.core.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

/**
 * This class has APIs for CRD tasks for AWS like SQS and DynamoDB
 */
public class AWSUtils
{
    // Log4j logger object
    private static final Logger logger = Logger.getLogger( AWSUtils.class );

    static
    {
        // initialize log4j
        init();
    }

    private AWSUtils()
    {
    }

    /**
     * Sends a message to SQS queue.
     * @param accessKey SQS access key
     * @param secretKey SQS secret key
     * @param region SQS region
     * @param queueUrl SQS URL
     * @param messageToSend Message to send
     * @param messageGroupId Message group id
     * @return True if message is sent successfully else False
     * @throws Exception
     */
    public static synchronized boolean sendMessageToSQS( String accessKey, String secretKey, String region, String queueUrl,
            String messageToSend, String messageGroupId )
    {
        logger.debug( "Entering AWSUtils.sendMessageToSQS() with: [" + accessKey + ", " + secretKey + ", " + region + ", " + queueUrl
                + ", " + messageToSend + ", " + messageGroupId + "]" );

        boolean isMessageSent = false;

        setAWSCredentials( accessKey, secretKey, region );
        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        SendMessageRequest sendMsgRequest = new SendMessageRequest().withQueueUrl( queueUrl ).withMessageBody( messageToSend )
                .withMessageGroupId( messageGroupId );
        SendMessageResult result = sqs.sendMessage( sendMsgRequest );
        if ( result.getMessageId() != null )
        {
            isMessageSent = true;
        }

        logger.debug( "Exiting AWSUtils.sendMessageToSQS() with: [" + isMessageSent + "]" );
        return isMessageSent;
    }

    /**
     * Gets a message to SQS queue.
     * @param accessKey SQS access key
     * @param secretKey SQS secret key
     * @param region SQS region
     * @param queueUrl SQS URL
     * @return List of SQS Message
     */
    public static synchronized List<Message> getMessageFromSQS( String accessKey, String secretKey, String region, String queueUrl )
    {
        logger.debug( "Entering AWSUtils.getMessageFromSQS() with: [" + accessKey + ", " + secretKey + ", " + region + ", " + queueUrl
                + "]" );

        setAWSCredentials( accessKey, secretKey, region );
        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest().withQueueUrl( queueUrl );
        List<Message> messages = sqs.receiveMessage( receiveMessageRequest ).getMessages();

        logger.debug( "Exiting AWSUtils.getMessageFromSQS() with: [" + messages + "]" );
        return messages;
    }

    /**
     * Deletes a single message from the queue with a receipt handle
     * @param accessKey SQS access key
     * @param secretKey SQS secret key
     * @param region SQS region
     * @param queueUrl SQS URL
     * @param receiptHandle Receipt handle of the message to be deleted
     * @return List of SQS Message
     */
    public static synchronized void deleteMessageFromSQS( String accessKey, String secretKey, String region, String queueUrl,
            String receiptHandle )
    {
        logger.debug( "Entering AWSUtils.deleteMessageFromSQS() with: [" + accessKey + ", " + secretKey + ", " + region + ", "
                + queueUrl + ", " + receiptHandle + "]" );

        setAWSCredentials( accessKey, secretKey, region );
        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

        sqs.deleteMessage( queueUrl, receiptHandle );

        logger.debug( "Exiting AWSUtils.deleteMessageFromSQS() with: []" );
    }

    /**
     * Gets all the messages from the queue and while doing that deletes each one as well
     * @param accessKey SQS access key
     * @param secretKey SQS secret key
     * @param region SQS region
     * @param queueUrl SQS URL
     * @return List of messages from the SQS queue
     */
    public static synchronized List<String> getAllMessagesFromQueue( String accessKey, String secretKey, String region,
            String queueUrl )
    {
        logger.debug( "Entering AWSUtils.getAllMessagesFromQueue() with: [" + accessKey + ", " + secretKey + ", " + region + ", "
                + queueUrl + "]" );

        List<String> messagesString = new ArrayList<>();
        boolean found = true;

        while ( found )
        {
            List<Message> messages = AWSUtils.getMessageFromSQS( accessKey, secretKey, region, queueUrl );
            if ( !messages.isEmpty() )
            {
                for ( Message message : messages )
                {
                    messagesString.add( message.getBody() );
                    logger.debug( "Message found: " + message.getBody() );
                    AWSUtils.deleteMessageFromSQS( accessKey, secretKey, region, queueUrl, message.getReceiptHandle() );
                }
            }
            else
            {
                found = false;
            }
        }

        logger.debug( "Exiting AWSUtils.getAllMessagesFromQueue() with: [" + messagesString + "]" );
        return messagesString;
    }

    /**
     * Loads the data into DynamoDB table
     * @param accessKey DynamoDB access key
     * @param secretKey DynamoDB secret key
     * @param region DynamoDB region
     * @param tableName DynamoDB table name
     * @param primaryKey DynamoDB table primary key
     * @param primaryKeyValue DynamoDB primary key value
     * @param restKey DynamoDB table other key
     * @param restKeyValueMap DynamoDB table other key value map
     * @return True if the loading was successful else False
     */
    public static synchronized boolean loadDataInDynamoDBTable( String accessKey, String secretKey, String region, String tableName,
            String primaryKey, String primaryKeyValue, String restKey, Map<String, Object> restKeyValueMap )
    {
        logger.debug( "Entering AWSUtils.loadDataInDynamoDBTable() with: [" + accessKey + ", " + secretKey + ", " + region + ", "
                + tableName + ", " + primaryKey + ", " + primaryKeyValue + ", " + restKey + ", " + restKeyValueMap + "]" );

        boolean isLoadingSuccess = false;

        setAWSCredentials( accessKey, secretKey, region );

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
        DynamoDB dynamoDB = new DynamoDB( client );
        Table table = dynamoDB.getTable( tableName );

        PutItemOutcome outcome = table
                .putItem( new Item().withPrimaryKey( primaryKey, primaryKeyValue ).withMap( restKey, restKeyValueMap ) );

        if ( outcome.getPutItemResult() != null )
            isLoadingSuccess = true;

        logger.debug( "Exiting AWSUtils.loadDataInDynamoDBTable() with: [" + isLoadingSuccess + "]" );
        return isLoadingSuccess;
    }

    /**
     * Gets the data from DynamoDB table
     * @param accessKey DynamoDB access key
     * @param secretKey DynamoDB secret key
     * @param region DynamoDB region
     * @param tableName DynamoDB table name
     * @param primaryKey DynamoDB table primary key
     * @param primaryKeyValue DynamoDB primary key value
     * @return DynamoDB Item corresponding to the primary key
     */
    public static synchronized Item getDataFromDynamoDBTable( String accessKey, String secretKey, String region, String tableName,
            String primaryKey, String primaryKeyValue )
    {
        logger.debug( "Entering AWSUtils.getDataFromDynamoDBTable() with: [" + accessKey + ", " + secretKey + ", " + region + ", "
                + tableName + ", " + primaryKey + ", " + primaryKeyValue + "]" );

        Item outcome = null;

        setAWSCredentials( accessKey, secretKey, region );

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
        DynamoDB dynamoDB = new DynamoDB( client );
        Table table = dynamoDB.getTable( tableName );

        GetItemSpec spec = new GetItemSpec().withPrimaryKey( primaryKey, primaryKeyValue );

        outcome = table.getItem( spec );

        logger.debug( "Exiting AWSUtils.getDataFromDynamoDBTable() with: [" + outcome + "]" );
        return outcome;
    }

    /**
     * Deletes the data from DynamoDB table
     * @param accessKey DynamoDB access key
     * @param secretKey DynamoDB secret key
     * @param region DynamoDB region
     * @param tableName DynamoDB table name
     * @param primaryKey DynamoDB table primary key
     * @param primaryKeyValue DynamoDB primary key value
     * @return True if the deletion was successful else False
     */
    public static synchronized boolean deleteDataFromDynamoDBTable( String accessKey, String secretKey, String region,
            String tableName, String primaryKey, String primaryKeyValue )
    {
        logger.debug( "Entering AWSUtils.deleteDataFromDynamoDBTable() with: [" + accessKey + ", " + secretKey + ", " + region + ", "
                + tableName + ", " + primaryKey + ", " + primaryKeyValue + "]" );

        boolean isDeleteSuccess = false;

        setAWSCredentials( accessKey, secretKey, region );

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
        DynamoDB dynamoDB = new DynamoDB( client );
        Table table = dynamoDB.getTable( tableName );

        DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey( primaryKey, primaryKeyValue );
        DeleteItemOutcome outcome = table.deleteItem( deleteItemSpec );

        if ( outcome != null )
            isDeleteSuccess = true;

        logger.debug( "Exiting AWSUtils.loadDataInDynamoDBTable() with: [" + isDeleteSuccess + "]" );
        return isDeleteSuccess;
    }

    /**
     * Sets the AWS credentials in the current session
     * @param accessKey Access key of the service
     * @param secretKey Secret key of the service
     * @param region Region of the service
     */
    private static synchronized void setAWSCredentials( final String accessKey, final String secretKey, final String region )
    {
        logger.debug( "Entering AWSUtils.setAWSCredentials() with: [" + accessKey + ", " + secretKey + ", " + region + "]" );

        System.clearProperty( "aws.accessKeyId" );
        System.clearProperty( "aws.secretKey" );
        System.clearProperty( "aws.region" );

        System.setProperty( "aws.accessKeyId", accessKey );
        System.setProperty( "aws.secretKey", secretKey );
        System.setProperty( "aws.region", region );

        logger.debug( "Exiting AWSUtils.setAWSCredentials() with: []" );
    }

    /**
     * Initializes log4j configurations
     */
    private static synchronized void init()
    {
        DOMConfigurator.configure( "log4j.xml" );
    }
}

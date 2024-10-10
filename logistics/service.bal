import ballerina/io;
import ballerinax/kafka;
import ballerina/lang.value;

type Package readonly & record {
    string customer_name;
    string contact_number;
    string pickup_location;
    string delivery_type;
    string preferred_times;
};

type Delivery readonly & record {
    string delivery_type;
    string delivery_time;
    string delivery_day;
};

configurable string groupId = "logistics";
configurable string new_delivery_request = "new-delivery-requests";
configurable string standard_delivery_request = "standard-delivery-requests";
configurable string express_delivery_request = "express-delivery-requests";
configurable string international_delivery_request = "internation-delivery-requests";
configurable decimal pollingInterval = 1;
configurable string kafkaEndpoint = "172.25.0.11:9092";

final kafka:ConsumerConfiguration consumerConfigs = {
    groupId: groupId,
    topics: [new_delivery_request],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval
};

service on new kafka:Listener(kafkaEndpoint, consumerConfigs) {
    private final kafka:Producer packageProducer;
    public function init() returns error? {
        self.packageProducer = check new (kafkaEndpoint);
    }

    remote function onConsumerRecord(kafka:AnydataConsumerRecord[] records) returns error? {
        foreach var item in records {
            byte[] byteArray = <byte[]> item.value;
            string jsonString = check string:fromBytes(byteArray);
            json Json = check value:fromJsonString(jsonString);
            Package package = check Json.cloneWithType(Package);

            string topic = "";
            match package.delivery_type{
                "standard" => {
                    topic = "standard-delivery-requests";
                }
                "express" => {
                    topic = "express-delivery-requests";
                }
                "international" => {
                    topic = "international-delivery-requests";
                }
            } 
            io:println("Sending package request to: '" + topic + "' topic");
            check self.packageProducer->send({
                topic: topic,
                value: package.toJsonString()
            });
        }
    }
}
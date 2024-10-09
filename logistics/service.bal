import ballerina/io;
import ballerinax/kafka;

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
    public function main() returns error? {
        kafka:Producer prod = check new (kafka:DEFAULT_URL);
        io:println("...");
        //check prod -> send({topic: "dsp", value: msg});
    }

    remote function onConsumerRecord(kafka:AnydataConsumerRecord[] records) returns error? {
        foreach var item in records {
           json value = item.value.toJson();
           Package package = check value.cloneWithType(Package);
           match package.delivery_type{
             "standard" => {
             }
             "express" => {
             }
            "international" => {
            }
           } 
        }
    }
}
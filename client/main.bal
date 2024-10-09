import ballerinax/kafka;
import ballerina/io;

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

configurable string groupId = "customers";
configurable string new_delivery_request = "new-delivery_request";
configurable string delivery_schedule_response = "delivery_schedule_response";
configurable decimal pollingInterval = 2;
configurable string kafkaEndpoint = kafka:DEFAULT_URL;

final kafka:ConsumerConfiguration consumerConfigs = {
    groupId: groupId,
    topics: [delivery_schedule_response],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval
};

service on new kafka:Listener(kafkaEndpoint, consumerConfigs) {
    private final kafka:Producer packageProducer;

    function init() returns error? {
        self.packageProducer = check new (kafkaEndpoint);
        Package new_package = {customer_name: "Showen Otto", contact_number: "0814503163", pickup_location: "Soweto Market", delivery_type: "standard", preferred_times: "Morning"};
        check self.packageProducer->send({
            topic: new_delivery_request,
            value: new_package.toJsonString()
        });
    }

    remote function onConsumerRecord(Delivery[] deliveries) returns error? {
        from Delivery delivery in deliveries
        do {
            io:println(delivery);
        };
    }
} 


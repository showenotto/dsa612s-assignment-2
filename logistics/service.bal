import ballerina/io;
import ballerinax/kafka;

public function main() returns error? {
    kafka:Producer prod = check new (kafka:DEFAULT_URL);
    io:println("l...");
    //check prod -> send({topic: "dsp", value: msg});
}
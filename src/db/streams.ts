export type EventType = 'INSERT' | 'MODIFY' | 'REMOVE';

export interface DynamoDBStreamEvent {
    Records: {
        eventName: EventType,
        dynamodb: {
            Keys: any,
            NewImage?: any
        }
    }[]
};

export interface EventBridgeType<EventName extends string, EventPayload> {
    Source: EventName,
    DetailType: `${EventName}.${Lowercase<EventType>}`,
    Detail?: {
        key: any,
        value?: EventPayload
    }
}
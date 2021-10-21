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

export interface EventBridgeType<EventName extends string, EventPayload, HashKeyType = {}, SortKeyType = {}> {
    Source: EventName,
    DetailType: `${EventName}.${Lowercase<EventType>}`,
    Detail?: {
        key: HashKeyType & SortKeyType,
        value?: HashKeyType & SortKeyType & EventPayload
    }
}
import { DynamoDBClient, GetItemCommand, PutItemCommand, DeleteItemCommand, UpdateItemCommand, TransactWriteItemsCommand, TransactWriteItem, QueryCommand, BatchWriteItemCommand, ScanCommand } from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';

export interface TransactionItemCompatibleCommand {
    toTransactionItem(): TransactWriteItem
}

function encodeNamesAndValues(obj: {[key: string]: any}, prefix?: string) {
    let names: {[key: string]: string} = {};
    let values: {[key: string]: any} = {};
    let mapping: {[key: string]: string} = {};

    const pre = prefix ?? '';

    Object.entries(obj).forEach(([k, v], i) => {
        const keyParts = k.split('.');
        const keyPartNames = keyParts.map((kp, j) => `#${pre}_${i}_${j}`);
        for (let j in keyParts) {
            names[keyPartNames[j]] = keyParts[j];
        }
        values[`:${pre}_${i}`] = v;
        mapping[keyPartNames.join('.')] = `:${pre}_${i}`;
    })

    values = marshall(values, { removeUndefinedValues: true });

    return { names, values, mapping };
}

export class CreateCommand<KeyType = any, ValueType = any> implements TransactionItemCompatibleCommand {
    item: any;
    conditionExpression: string;

    constructor(
        private dynamoDBClient: DynamoDBClient,
        private tableName: string,
        key: KeyType,
        value: ValueType
    ) {
        this.item = marshall({ ...value, ...key }, { removeUndefinedValues: true });
        this.conditionExpression = `attribute_not_exists(${Object.keys(key)[0]})`;
    }

    async send(): Promise<ValueType> {
        await this.dynamoDBClient.send(new PutItemCommand({
            TableName: this.tableName,
            Item: this.item,
            ConditionExpression: this.conditionExpression
        }));
        return this.data();
    }

    data() {
        return unmarshall(this.item) as ValueType;
    }

    toTransactionItem(): TransactWriteItem {
        return {
            Put: {
                TableName: this.tableName,
                Item: this.item,
                ConditionExpression: this.conditionExpression
            }
        }
    }
}

export class BatchWriteCommand<KeyType = any, ValueType = any> {
    batches: any[][] = [];

    constructor(
        private dynamoDBClient: DynamoDBClient,
        private tableName: string,
        records: {
            key: KeyType,
            value: ValueType
        }[]
    ) {
        let longListItems = records.map(
            r => marshall({ ...r.value, ...r.key }, { removeUndefinedValues: true })
        );
        
        while (longListItems.length > 0) {
            this.batches.push(longListItems.splice(0, 25));
        }
    }

    private async sendWithRetries(requestItems: any, retryLeft: number, backoffMs: number) {
        if (retryLeft <= 0) {
            throw new Error('Could not complete BatchWrite after exhausting all retries');
        }

        const res = await this.dynamoDBClient.send(new BatchWriteItemCommand({
            RequestItems: requestItems
        }));

        if (res.UnprocessedItems?.[this.tableName]?.length > 0) {
            await new Promise((resolve) => setTimeout(resolve, backoffMs));
            return this.sendWithRetries(res.UnprocessedItems, retryLeft - 1, backoffMs * 2);
        }

        return res;
    }

    async send() {
        return await Promise.all(this.batches.map(
            batch => this.sendWithRetries({
                [this.tableName]: batch.map(
                    item => ({
                        PutRequest: {
                            Item: item
                        }
                    })
                )
            }, 5, 10)
        ));
    }
}

export interface RetrieveCommandOptions {
    consistentRead?: boolean
}

export class RetrieveCommand<KeyType = any, ValueType = any> {
    key: any;
    consistentRead: boolean;

    constructor(
        private dynamoDBClient: DynamoDBClient,
        private tableName: string,
        key: KeyType,
        options?: RetrieveCommandOptions
    ) {
        this.key = marshall(key);
        this.consistentRead = options?.consistentRead ?? false;
    }

    async send(): Promise<KeyType & ValueType | null> {
        const item = await this.dynamoDBClient.send(new GetItemCommand({
            TableName: this.tableName,
            Key: this.key,
            ConsistentRead: this.consistentRead
        }));

        if (item?.Item) {
            return unmarshall(item.Item) as KeyType & ValueType;
        } else {
            return null;
        }
    }
}

export interface UpdateCommandOptions {
    additionalConditions?: {[key: string]: any}
}

export class UpdateCommand<KeyType = any, ValueType = any> implements TransactionItemCompatibleCommand {
    key: any;
    updateExpression: string;
    expressionAttributesNames: {[key: string]: string};
    expressionAttributeValues: {[key: string]: any};
    conditionExpression: string;

    constructor(
        private dynamoDBClient: DynamoDBClient,
        private tableName: string,
        key: KeyType,
        set: {[key: string]: any},
        options?: UpdateCommandOptions
    ) {
        this.key = marshall(key);

        const { names, values, mapping } = encodeNamesAndValues(set);
        this.expressionAttributeValues = values;
        this.expressionAttributesNames = names;
        this.updateExpression = `SET ${Object.entries(mapping).map(([k, v]) => `${k} = ${v}`).join(', ')}`;

        let conditions = [
            `attribute_exists(${Object.keys(key)[0]})`
        ];
        if (options?.additionalConditions) {
            const { names, values, mapping } = encodeNamesAndValues(options.additionalConditions, 'cond');
            this.expressionAttributeValues = {
                ...this.expressionAttributeValues,
                ...values
            };
            this.expressionAttributesNames = {
                ...this.expressionAttributesNames,
                ...names
            };
            conditions = conditions.concat(
                ...Object.entries(mapping).map(([k, v]) => `${k} = ${v}`)
            );
        }
        if (conditions.length > 0) {
            this.conditionExpression = conditions.join(' AND ');
        }
    }

    async send(): Promise<KeyType & ValueType | null> {
        const item = await this.dynamoDBClient.send(new UpdateItemCommand({
            TableName: this.tableName,
            Key: this.key,
            UpdateExpression: this.updateExpression,
            ExpressionAttributeNames: this.expressionAttributesNames,
            ExpressionAttributeValues: this.expressionAttributeValues,
            ConditionExpression: this.conditionExpression,
            ReturnValues: 'ALL_NEW'
        }));

        if (item?.Attributes) {
            return unmarshall(item.Attributes) as KeyType & ValueType;
        } else {
            return null;
        }
    }

    toTransactionItem(): TransactWriteItem {
        return {
            Update: {
                TableName: this.tableName,
                Key: this.key,
                UpdateExpression: this.updateExpression,
                ExpressionAttributeNames: this.expressionAttributesNames,
                ExpressionAttributeValues: this.expressionAttributeValues,
                ConditionExpression: this.conditionExpression
            }
        }
    }
}

export interface ReplaceCommandOptions {
    additionalConditions?: {[key: string]: any}
}

export class ReplaceCommand<KeyType = any, ValueType = any> implements TransactionItemCompatibleCommand {
    item: any;
    expressionAttributesNames: {[key: string]: string};
    expressionAttributeValues: {[key: string]: any};
    conditionExpression: string;

    constructor(
        private dynamoDBClient: DynamoDBClient,
        private tableName: string,
        key: KeyType,
        value: ValueType,
        options?: ReplaceCommandOptions
    ) {
        this.item = marshall({ ...value, ...key }, { removeUndefinedValues: true });

        let conditions = [
            `attribute_exists(${Object.keys(key)[0]})`
        ];
        if (options?.additionalConditions) {
            const { names, values, mapping } = encodeNamesAndValues(options.additionalConditions);
            this.expressionAttributeValues = values;
            this.expressionAttributesNames = names;
            conditions = conditions.concat(
                ...Object.entries(mapping).map(([k, v]) => `${k} = ${v}`)
            );
        }
        if (conditions.length > 0) {
            this.conditionExpression = conditions.join(' AND ');
        }
    }

    async send(): Promise<KeyType & ValueType> {
        await this.dynamoDBClient.send(new PutItemCommand({
            TableName: this.tableName,
            Item: this.item,
            ExpressionAttributeNames: this.expressionAttributesNames,
            ExpressionAttributeValues: this.expressionAttributeValues,
            ConditionExpression: this.conditionExpression
        }));
        return unmarshall(this.item) as KeyType & ValueType;
    }

    toTransactionItem(): TransactWriteItem {
        return {
            Put: {
                TableName: this.tableName,
                Item: this.item,
                ExpressionAttributeNames: this.expressionAttributesNames,
                ExpressionAttributeValues: this.expressionAttributeValues,
                ConditionExpression: this.conditionExpression,
            }
        }
    }
}

export interface DeleteCommandOptions {
    additionalConditions?: {[key: string]: any}
}

export class DeleteCommand<KeyType = any> implements TransactionItemCompatibleCommand {
    key: any;
    expressionAttributesNames: {[key: string]: string};
    expressionAttributeValues: {[key: string]: any};
    conditionExpression: string;

    constructor(
        private dynamoDBClient: DynamoDBClient,
        private tableName: string,
        key: KeyType,
        options?: DeleteCommandOptions
    ) {
        this.key = marshall(key);


        let conditions = [
            `attribute_exists(${Object.keys(key)[0]})`
        ];
        if (options?.additionalConditions) {
            const { names, values, mapping } = encodeNamesAndValues(options.additionalConditions);
            this.expressionAttributeValues = values;
            this.expressionAttributesNames = names; 
            conditions = conditions.concat(
                ...Object.entries(mapping).map(([k, v]) => `${k} = ${v}`)
            );
        } 
        if (conditions.length > 0) {
            this.conditionExpression = conditions.join(' AND ');
        }
    }

    async send(): Promise<void> {
        await this.dynamoDBClient.send(new DeleteItemCommand({
            TableName: this.tableName,
            Key: this.key,
            ExpressionAttributeNames: this.expressionAttributesNames,
            ExpressionAttributeValues: this.expressionAttributeValues,
            ConditionExpression: this.conditionExpression
        }));
    }

    toTransactionItem(): TransactWriteItem {
        return {
            Delete: {
                TableName: this.tableName,
                Key: this.key,
                ExpressionAttributeNames: this.expressionAttributesNames,
                ExpressionAttributeValues: this.expressionAttributeValues,
                ConditionExpression: this.conditionExpression
            }
        }
    }
}

export interface ListCommandResult<ValueType = any> {
    count: number,
    cursor?: any,
    items: ValueType[]
}

type MonoLeafCriterionOperator = 'begins_with' | '>' | '>=' | '<' | '<=' | '=';
type DuoLeafCriterionOperator = 'BETWEEN';
export type LeafCriterion<FilterType = any> = {
    name: keyof FilterType,
    operator: MonoLeafCriterionOperator,
    value: FilterType[LeafCriterion<FilterType>['name']]

} | {
    name: keyof FilterType,
    operator: DuoLeafCriterionOperator,
    leftValue: FilterType[LeafCriterion<FilterType>['name']],
    rightValue: FilterType[LeafCriterion<FilterType>['name']]
};

type ResolvedLeafCriterion = LeafCriterion<{[key: string]: string}>;

function leafCriterionToCondition(resolvedCriterion: ResolvedLeafCriterion) { 
    if (resolvedCriterion.operator === 'BETWEEN') {
        return `${resolvedCriterion.name} BETWEEN ${resolvedCriterion.leftValue} AND ${resolvedCriterion.rightValue}`
    }
    if (resolvedCriterion.operator === 'begins_with') {
        return `begins_with(${resolvedCriterion.name}, ${resolvedCriterion.value})`;
    }
    return `${resolvedCriterion.name} ${resolvedCriterion.operator} ${resolvedCriterion.value}`;
}

function encodeLeafCriterion<FilterType>(
    criterion: LeafCriterion<FilterType>,
    prefix: string,
    names: {[key: string]: string},
    values: {[key: string]: any},
    namePrefix?: string
): ResolvedLeafCriterion {
    const pre = namePrefix ? `${namePrefix}.` : '';

    names[`#${prefix}`] = criterion.name as string;

    if (criterion.operator === 'BETWEEN') {
        Object.entries(marshall({
            [`:${prefix}_l`]: criterion.leftValue,
            [`:${prefix}_r`]: criterion.rightValue,
        })).forEach(([k, v]) => {
            values[k] = v
        });
        return {
            operator: 'BETWEEN',
            name: `${pre}#${prefix}`,
            leftValue: `:${prefix}_l`,
            rightValue: `:${prefix}_r`
        };
    }
    Object.entries(marshall({
        [`:${prefix}`]: criterion.value
    })).forEach(([k, v]) => {
        values[k] = v
    });
    return {
        operator: criterion.operator,
        name: `${pre}#${prefix}`,
        value: `:${prefix}`
    };
}

export type SortKeyCriterion<SortKeyType> = LeafCriterion<SortKeyType>;

export interface ListCommandOptions<SortKeyType, FilterType> {
    indexName?: string,
    limit?: number,
    from?: any,
    ascending?: boolean,
    count?: boolean,
    sortKeyCriterion?: SortKeyCriterion<SortKeyType>,
    filterCriteria?: LeafCriterion<FilterType>[][]
}

export class ListCommand<HashKeyType = any, SortKeyType = any, ValueType = any, FilterType = any> {
    keyConditionExpression: string;
    filterExpression?: string;
    expressionAttributesNames: {[key: string]: string};
    expressionAttributeValues: {[key: string]: any};
    indexName?: string;
    limit?: number;
    ascendingScan: boolean;
    count?: boolean;
    exclusiveStartKey?: any; 

    constructor(
        private dynamoDBClient: DynamoDBClient,
        private tableName: string,
        hashKey: HashKeyType,
        options?: ListCommandOptions<SortKeyType, FilterType>,
        attributesPrefix?: string
    ) {
        this.indexName = options?.indexName;
        this.limit = options?.limit;
        this.ascendingScan = options?.ascending ?? true;
        this.count = options?.count;
        this.exclusiveStartKey = options?.from;

        const { names, values, mapping } = encodeNamesAndValues(hashKey);
        this.expressionAttributeValues = values;
        this.expressionAttributesNames = names;
        if (Object.entries(mapping).length != 1) {
            throw Error(`hashKey should only have one field, got: ${JSON.stringify(hashKey)}`);
        }
        let keyConditions = [
            Object.entries(mapping)[0].join(' = ')
        ];
        if (options?.sortKeyCriterion) {
            keyConditions.push(this.computeKeyConditions(options.sortKeyCriterion));
        }
        if (keyConditions.length > 0) {
            this.keyConditionExpression = keyConditions.join(' AND ');
        }

        if (options?.filterCriteria) {
            this.filterExpression = this.computeFilterExpression(options.filterCriteria, attributesPrefix);
        }
    }

    private computeKeyConditions(sortKeyCriterion: SortKeyCriterion<SortKeyType>): string {
        return leafCriterionToCondition(
            encodeLeafCriterion(
                sortKeyCriterion,
                'skc',
                this.expressionAttributesNames,
                this.expressionAttributeValues
            )
        );
    }

    private computeFilterExpression(filterCriteria: LeafCriterion<FilterType>[][], attributesPrefix?: string): string {
        return filterCriteria.map((andConditions, idx) => 
            `(${andConditions.map((cond, jdx) => 
                leafCriterionToCondition(
                    encodeLeafCriterion(
                        cond,
                        `fc_${idx}_${jdx}`,
                        this.expressionAttributesNames,
                        this.expressionAttributeValues,
                        attributesPrefix
                    )
                )
            ).join(' AND ')})`
        ).join(' OR ');
    }

    async send(): Promise<ListCommandResult<KeyType & ValueType>> {
        const results = await this.dynamoDBClient.send(new QueryCommand({
            TableName: this.tableName,
            KeyConditionExpression: this.keyConditionExpression,
            FilterExpression: this.filterExpression,
            ExpressionAttributeNames: this.expressionAttributesNames,
            ExpressionAttributeValues: this.expressionAttributeValues,
            ExclusiveStartKey: this.exclusiveStartKey,
            ScanIndexForward: this.ascendingScan,
            IndexName: this.indexName,
            Limit: this.limit,
            Select: this.count ? 'COUNT' : undefined
        }));

        return {
            count: results?.Count ?? 0,
            cursor: results?.LastEvaluatedKey,
            items: (results?.Items?.map((item) => unmarshall(item)).map((item) => {
                return {
                    ...item,
                    ...(
                        (item.projections && !item.attributes) ? {
                            attributes: item.projections
                        } : {}
                    )
                };
            }) ?? []) as (KeyType & ValueType)[]
        };
    }
}

export interface FullScanCommandResult<ValueType = any> {
    count: number,
    cursor?: any,
    items: ValueType[]
}

export interface FullScanCommandOptions {
    from?: any
}

export class FullScanCommand<HashKeyType = any, SortKeyType = any, ValueType = any> {
    exclusiveStartKey: any; 

    constructor(
        private dynamoDBClient: DynamoDBClient,
        private tableName: string,
        options?: FullScanCommandOptions
    ) {
        if (options?.from) {
            this.exclusiveStartKey = options.from;
        }
    }

    async send(): Promise<FullScanCommandResult<KeyType & ValueType>> {
        const results = await this.dynamoDBClient.send(new ScanCommand({
            TableName: this.tableName,
            ExclusiveStartKey: this.exclusiveStartKey
        }));

        return {
            count: results?.Count ?? 0,
            cursor: results?.LastEvaluatedKey,
            items: (results?.Items?.map((item) => unmarshall(item)).map((item) => {
                return {
                    ...item,
                    ...(
                        (item.projections && !item.attributes) ? {
                            attributes: item.projections
                        } : {}
                    )
                };
            }) ?? []) as (KeyType & ValueType)[]
        };
    }
}

export class Transaction {
    static async run(dynamoDBClient: DynamoDBClient, commands: TransactionItemCompatibleCommand[]): Promise<void> {
        await dynamoDBClient.send(new TransactWriteItemsCommand({ 
            TransactItems: commands.map((c) => c.toTransactionItem())
        }));
    }
}
import { DeleteCommandOptions, ListCommand, ListCommandOptions, UpdateCommandOptions } from "./db/commands";
import { filterResourceFields, ResourceTable } from "./db/resource-table";
import { hasHeaders, HttpRequest, HttpRequestAsyncFunc } from "./http";
import { uuid } from "./utils";

function isExtensibleAttributes(data: any): data is Object {
    return data instanceof Object;
} 

function defaultIdCreator(r: HttpRequest) {
    return uuid();
}

export function postResource<H, S, HT, ST, A>(props: {
    table: ResourceTable<H, S, HT, ST, A>,
    keyFn: (r: HttpRequest, id: string) => H & S,
    idFn?: (r: HttpRequest) => string,
    overrideFields?: (r: HttpRequest, data: any) => any
}): HttpRequestAsyncFunc {
    return async (r: HttpRequest) => {
        const data = r.bodyAsJson();
        if (!isExtensibleAttributes(data)) {
            console.error('Data is not an object');
            return {
                statusCode: 500
            };
        }
        const id = props.idFn ? props.idFn(r) : defaultIdCreator(r);
        const value = await props.table.createCommand(
            id,
            props.keyFn(r, id), 
            props.overrideFields ? props.overrideFields(r, data) : data
        ).send();
        return {
            statusCode: 201,
            headers: {
                ETag: value.meta.etag
            },
            data: filterResourceFields(value)
        };
    };
}

export function getResource<H, S, HT, ST, A>(props: {
    table: ResourceTable<H, S, HT, ST, A>,
    keyFn: (r: HttpRequest) => H & S,
    shouldBeHidden?: (r: HttpRequest, p: any) => boolean
}): HttpRequestAsyncFunc {
    return async (r: HttpRequest) => {
        const value = await props.table.retrieveCommand(props.keyFn(r)).send();
        if (!value) {
            return {
                statusCode: 404
            };
        }
        if (props.shouldBeHidden && props.shouldBeHidden(r, value)) {
            console.info(`Hidden resource: ${props.keyFn(r)}`)
            return {
                statusCode: 404
            };
        }
        return {
            statusCode: 200,
            headers: {
                ETag: value.meta.etag
            },
            data: filterResourceFields(value)
        };
    };
}

export const hasIfMatchHeader = hasHeaders(['if-match']);

function fetchETag(r: HttpRequest): string | undefined {
    if (!r.headers) {
        return undefined;
    }
    return r.headers!['if-match']?.toString();
}

export function putResource<H, S, HT, ST, A>(props: {
    table: ResourceTable<H, S, HT, ST, A>,
    keyFn: (r: HttpRequest) => H & S,
    updateOptions?: (r: HttpRequest) => UpdateCommandOptions
}): HttpRequestAsyncFunc {
    return async (r: HttpRequest) => {
        const data = r.bodyAsJson();
        if (!isExtensibleAttributes(data)) {
            console.error('Data is not an object');
            return {
                statusCode: 500
            };
        }
        try {
            const etag = fetchETag(r);
            const value = await props.table.updateCommand(
                props.keyFn(r), 
                data,
                {
                    ...(props.updateOptions ? props.updateOptions(r) : {}),
                    ...(etag ? {
                        additionalConditions: {
                            'meta.etag': etag
                        }
                    } : {})
                }
            ).send();

            if (!value) {
                return {
                    statusCode: 404
                };
            }
            return {
                statusCode: 200,
                headers: {
                    ETag: value.meta.etag
                },
                data: filterResourceFields(value)
            };
        } catch (err) {
            if (err.name === 'ConditionalCheckFailedException') {
                console.info(err);
                return {
                    statusCode: 409
                }; 
            }
            throw err;
        }
    };
}

export function deleteResource<H, S, HT, ST, A>(props: {
    table: ResourceTable<H, S, HT, ST, A>,
    keyFn: (r: HttpRequest) => H & S,
    deleteOptions?: (r: HttpRequest) => DeleteCommandOptions
}): HttpRequestAsyncFunc {
    return async (r: HttpRequest) => {
        try {
            const etag = fetchETag(r);
            await props.table.deleteCommand(
                props.keyFn(r), 
                {
                    ...(props.deleteOptions ? props.deleteOptions(r) : {}),
                    ...(etag ? {
                        additionalConditions: {
                            'meta.etag': etag
                        }
                    } : {})
                }
            ).send();
            return {
                statusCode: 200
            };
        } catch (err) {
            if (err.name === 'ConditionalCheckFailedException') {
                console.info(err);
                return {
                    statusCode: 409
                }; 
            }
            throw err;
        }
    };
}

interface Listable<H, HT, ST> {
  listCommand(hashKey: H, options?: Omit<ListCommandOptions<ST>, 'indexName'>): ListCommand<HT, ST, any>;
} 

type SortOrder = 'asc' | 'desc';
const orderToAscending: {[k in SortOrder]: boolean} = {
    asc: true,
    desc: false
};
export function listResources<H, HT, ST>(props: {
    listable: Listable<H, HT, ST>,
    hashKeyFn: (r: HttpRequest) => H,
    optionsFn?: (r: HttpRequest) => Omit<ListCommandOptions<ST>, 'indexName'>
}): HttpRequestAsyncFunc {
    return async (r: HttpRequest) => {
        let ascending = false;
        let limit = 10;
        let cursor: string | undefined = undefined;

        let rawCursor = r.queryParams!['page[cursor]'];
        let rawOrdering = r.queryParams!['ordering'];

        const rawPageSize = r.queryParams!['page[size]'];
        if (rawPageSize) {
            try {
                limit = Math.min(
                    Math.max(
                        0,
                        parseInt(rawPageSize)
                    ), 100
                );
            } catch (e) {
                console.info(`Error while trying to parse page[size] parameter: ${e}`);
                return {
                    statusCode: 400,
                    errors: [{
                        title: 'Bad Request',
                        detail: 'Parameter must be an integer',
                        source: {
                            parameter: 'page[size]'
                        }
                    }]
                }; 
            }
        }

        if (rawOrdering) {
            ascending = orderToAscending[rawOrdering as SortOrder];
            if (ascending === undefined) {
                return {
                    statusCode: 400,
                    errors: [{
                        title: 'Bad Request',
                        detail: 'Parameter must be equal to asc or desc',
                        source: {
                            parameter: 'ordering'
                        }
                    }]
                }; 
            }
        }

        try {
            if (rawCursor) {
                cursor = JSON.parse(Buffer.from(rawCursor, 'base64').toString('ascii'));
            }
        } catch(err) {
            return {
                statusCode: 400,
                errors: [{
                    title: 'Bad Request',
                    detail: 'Bad cursor value',
                    source: {
                        parameter: 'page[cursor]' 
                    }
                }]
            };
        }

        const resp = await props.listable.listCommand(
            props.hashKeyFn(r),
            {
                limit,
                from: cursor,
                ascending,
                ...(props.optionsFn ? props.optionsFn(r) : {})
            }
        ).send();

        const nextCursor = resp.cursor ? Buffer.from(JSON.stringify(resp.cursor)).toString('base64') : undefined;
        const orderParam: SortOrder = ascending ? 'asc' : 'desc';

        return {
            statusCode: 200,
            ...(
                nextCursor ? {
                    meta: {
                        page: {
                            cursor: nextCursor
                        }
                    }
                } : {}
            ),
            links: {
                ...(
                    nextCursor ? {
                        next: `${r.event.rawPath}?ordering=${orderParam}&page[size]=${limit}&page[cursor]=${nextCursor}`
                    } : {}
                )
            },
            data: resp.items.map(filterResourceFields)
        };
    };
}
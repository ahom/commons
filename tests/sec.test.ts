import { SecurityContext } from "../src/sec";

describe('SecurityContext', () => {
    test('No active subscription', () => {
        expect(new SecurityContext({}).hasActiveSubscription()).toBeFalsy();
    });
    test('Active subscription', () => {
        expect(new SecurityContext({ subscription: { id: 'lal', ownerId: 'lil', itemId: 'lul' } }).hasActiveSubscription()).toBeTruthy();
    });
    test('No valid user', () => {
        expect(new SecurityContext({}).hasValidUser()).toBeFalsy();
    });
    test('Valid user', () => {
        expect(new SecurityContext({ userId: 'lul' }).hasValidUser()).toBeTruthy();
    });
    test('No scope authorized', () => {
        expect(new SecurityContext({}).isScopeAuthorized('lol')).toBeFalsy();
    });
    test('One scope authorized', () => {
        expect(new SecurityContext({ scopes: ['lol'] }).isScopeAuthorized('lol')).toBeTruthy();
    });
    test('One scope not authorized', () => {
        expect(new SecurityContext({ scopes: ['lol'] }).isScopeAuthorized('lil')).toBeFalsy();
    });
    test('Multiple scopes authorized', () => {
        expect(new SecurityContext({ scopes: ['lol', 'lul', 'lil'] }).isScopeAuthorized('lul')).toBeTruthy();
    });
    test('Multiple scopes not authorized', () => {
        expect(new SecurityContext({ scopes: ['lol', 'lul', 'lil'] }).isScopeAuthorized('lyl')).toBeFalsy();
    });
    test('Wildcard scope authorized', () => {
        expect(new SecurityContext({ scopes: ['lol', 'lul/*', 'lil'] }).isScopeAuthorized('lul/read')).toBeTruthy();
    });
    test('Wildcard scope not authorized', () => {
        expect(new SecurityContext({ scopes: ['lol', 'lul/*', 'lil'] }).isScopeAuthorized('lul')).toBeFalsy();
    });

    test('Empty values fromAuthorizer', () => {
        expect(SecurityContext.fromAuthorizer(undefined).props).toEqual({});
    });
    test('Proper mapping fromAuthorizer', () => {
        expect(SecurityContext.fromAuthorizer({
            lambda: {
                applicationId: 'APP',
                subscription: {
                    id: 'SUB',
                    ownerId: 'OWN',
                    itemId: 'ITM'
                },
                userId: 'USR',
                scopes: ['SCO']
            }
        }).props).toEqual({
            applicationId: 'APP',
            subscription: {
                id: 'SUB',
                ownerId: 'OWN',
                itemId: 'ITM'
            },
            userId: 'USR',
            scopes: ['SCO']
        });
    });
});
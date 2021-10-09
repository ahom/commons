import { filterScope, filterScopes, isScopeIncludedIn, SecurityContext } from "../src/sec";

describe('Scopes', () => {
    test('Scope not included', () => {
        expect(isScopeIncludedIn('lol', 'lil')).toBeFalsy();
        expect(isScopeIncludedIn('lol', 'loli')).toBeFalsy();
        expect(isScopeIncludedIn('lol', 'lo')).toBeFalsy();
        expect(isScopeIncludedIn('lol', 'lol/*')).toBeFalsy();
        expect(isScopeIncludedIn('lol', 'lo/*')).toBeFalsy();
        // we do not allow stars in the middle, only at the end
        expect(isScopeIncludedIn('lol', 'l*l*')).toBeFalsy();
    });
    test('Scope included', () => {
        expect(isScopeIncludedIn('lol', 'lol')).toBeTruthy();
        expect(isScopeIncludedIn('lol', 'lo*')).toBeTruthy();
        expect(isScopeIncludedIn('lol', 'l*')).toBeTruthy();
        expect(isScopeIncludedIn('lol', '*')).toBeTruthy();
    });
    test('Scope filtered', () => {
        expect(filterScope('lol', 'lil')).toBeUndefined();
        expect(filterScope('lol', 'lol')).toEqual('lol');
        expect(filterScope('lol', 'lo*')).toEqual('lol');
        expect(filterScope('lo*', 'lol')).toEqual('lol');
    });
    test('Scopes filtered', () => {
        expect(filterScopes(
            ['lol', 'lil', 'lul', 'lo*'],
            ['lo*', 'lil']
        )).toEqual(
            ['lol', 'lil', 'lo*']
        )
    });
});

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
                email: 'YEP',
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
            email: 'YEP',
            scopes: ['SCO']
        });
    });
});
////////////////////////////////////////////////////////////////////////////
//
// Copyright 2014 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

import Realm
import Realm.Private

/**
Migration block used to migrate a Realm.

:param: migration        `Migration` object used to perform the migration. The
                         migration object allows you to enumerate and alter any
                         existing objects which require migration.
:param: oldSchemaVersion The schema version of the `Realm` being migrated.
*/
public typealias MigrationBlock = (migration: Migration, oldSchemaVersion: UInt) -> Void

/**
Provides both the old and new versions of an object in this Realm. Objects properties can only be
accessed using subscripting.

:param: oldObject Object in original `Realm` (read-only)
:param: newObject Object in migrated `Realm` (read-write)
*/
public typealias MigrationObjectEnumerateBlock = (oldObject: MigrationObject, newObject: MigrationObject) -> Void

/**
Specify a schema version and an associated migration block which is applied when
opening the default Realm with an old schema version.

Before you can open an existing `Realm` which has a different on-disk schema
from the schema defined in your object interfaces, you must provide a migration
block which converts from the disk schema to your current object schema. At the
minimum your migration block must initialize any properties which were added to
existing objects without defaults and ensure uniqueness if a primary key
property is added to an existing object.

You should call this method before accessing any `Realm` instances which
require migration. After registering your migration block, Realm will call your
block automatically as needed.

:warning: Unsuccessful migrations will throw exceptions when the migration block is applied.
          This will happen in the following cases:

          - The migration block was run and returns a schema version which is not higher
            than the previous schema version.
          - A new property without a default was added to an object and not initialized
            during the migration. You are required to either supply a default value or to
            manually populate added properties during a migration.

:param: version The current schema version.
:param: block   The block which migrates the Realm to the current version.
*/
public func setDefaultRealmSchemaVersion(schemaVersion: UInt, migrationBlock: MigrationBlock) {
    RLMRealm.setDefaultRealmSchemaVersion(schemaVersion, withMigrationBlock: accessorMigrationBlock(migrationBlock))
}

/**
Specify a schema version and an associated migration block which is applied when
opening a Realm at the specified path with an old schema version.

Before you can open an existing `Realm` which has a different on-disk schema
from the schema defined in your object interfaces, you must provide a migration
block which converts from the disk schema to your current object schema. At the
minimum your migration block must initialize any properties which were added to
existing objects without defaults and ensure uniqueness if a primary key
property is added to an existing object.

You should call this method before accessing any `Realm` instances which
require migration. After registering your migration block, Realm will call your
block automatically as needed.

:warning: Unsuccessful migrations will throw exceptions when the migration block is applied.
          This will happen in the following cases:

          - The migration block was run and returns a schema version which is not higher
            than the previous schema version.
          - A new property without a default was added to an object and not initialized
            during the migration. You are required to either supply a default value or to
            manually populate added properties during a migration.

:param: version   The current schema version.
:param: realmPath The path of the Realms to migrate.
:param: block     The block which migrates the Realm to the current version.
*/
public func setSchemaVersion(schemaVersion: UInt, realmPath: String, migrationBlock: MigrationBlock) {
    RLMRealm.setSchemaVersion(schemaVersion, forRealmAtPath: realmPath, withMigrationBlock: accessorMigrationBlock(migrationBlock))
}

/**
Get the schema version for a Realm at a given path.
:param: realmPath     Path to a Realm file.
:param: encryptionKey Optional 64-byte encryption key for encrypted Realms.
:param: error         If an error occurs, upon return contains an `NSError` object
                      that describes the problem. If you are not interested in
                      possible errors, omit the argument, or pass in `nil`.
:returns: The version of the Realm at `realmPath` or `nil` if the version cannot be read.
*/
public func schemaVersionAtPath(realmPath: String, encryptionKey: NSData? = nil, error: NSErrorPointer = nil) -> UInt? {
    let version = RLMRealm.schemaVersionAtPath(realmPath, encryptionKey: encryptionKey, error: error)
    if version == RLMNotVersioned {
        return nil
    }
    return version
}

/**
Performs the registered migration block on a Realm at the given path.

This method is called automatically when opening a Realm for the first time and does
not need to be called explicitly. You can choose to call this method to control
exactly when and how migrations are performed.

:param: path          The path of the Realm to migrate.
:param: encryptionKey Optional 64-byte encryption key for encrypted Realms.
                      If the Realms at the given path are not encrypted, omit the argument or pass
                      in `nil`.

:returns: `nil` if the migration was successful, or an `NSError` object that describes the problem
          that occured otherwise.
*/
public func migrateRealm(path: String, encryptionKey: NSData? = nil) -> NSError? {
    if encryptionKey == nil {
        return RLMRealm.migrateRealmAtPath(path)
    }
    else {
        return RLMRealm.migrateRealmAtPath(path, encryptionKey: encryptionKey)
    }
}

/**
`Migration` is the object passed into a user-defined `MigrationBlock` when updating the version
of a `Realm` instance.

This object provides access to the previous and current `Schema`s for this migration.
*/
public final class Migration {

    // MARK: Properties

    /// The migration's old `Schema`, describing the `Realm` before applying a migration.
    public var oldSchema: Schema { return Schema(rlmSchema: rlmMigration.oldSchema) }

    /// The migration's new `Schema`, describing the `Realm` after applying a migration.
    public var newSchema: Schema { return Schema(rlmSchema: rlmMigration.newSchema) }

    private var rlmMigration: RLMMigration

    // MARK: Altering Objects During a Migration

    /**
    Enumerates objects of a given type in this Realm, providing both the old and new versions of
    each object. Object properties can be accessed using subscripting.

    :warning: All objects returned are of a type specific to the current migration and should not be
              casted to the normal object type. Instead you should access them as `Object`s and use
              subscripting to access properties.
    
    :param: className The name of the `Object` class to enumerate.
    :param: block     The block providing both the old and new versions of an object in this Realm.
    */
    public func enumerate(objectClassName: String, block: MigrationObjectEnumerateBlock) {
        rlmMigration.enumerateObjects(objectClassName, block: {
            block(oldObject: unsafeBitCast($0, MigrationObject.self), newObject: unsafeBitCast($1, MigrationObject.self));
        })
    }

    /**
    Create an `Object` of type `className` in the Realm being migrated.

    :param: className The name of the `Object` class to create.
    :param: object    The object used to populate the object. This can be any key/value coding
                      compliant object, or a JSON object such as those returned from the methods in
                      `NSJSONSerialization`, or an `Array` with one object for each persisted
                      property. An exception will be thrown if any required properties are not
                      present and no default is set.
    
    :returns: The created object.
    */
    public func create(className: String, withObject object: AnyObject) -> MigrationObject {
        return unsafeBitCast(rlmMigration.createObject(className, withObject: object), MigrationObject.self)
    }

    /**
    Delete an object from a Realm during a migration. This can be called within
    `enumerate(_:block:)`.

    :param: object Object to be deleted from the Realm being migrated.
    */
    public func delete(object: MigrationObject) {
        RLMDeleteObjectFromRealm(object)
    }

    private init(_ rlmMigration: RLMMigration) {
        self.rlmMigration = rlmMigration
    }
}

/// Object interface which allows untyped getters and setters for Objects during a migration.
public final class MigrationObject : Object {

    private var listProperties = [String: List<MigrationObject>]()

    /// Returns the value of the property with the given name.
    subscript(key: String) -> AnyObject? {
        get {
            if (self.objectSchema[key]?.type == RLMPropertyType.Array) {
                return listProperties[key]
            }
            return super[key]
        }
        set(value) {
            if (self.objectSchema[key]?.type == RLMPropertyType.Array) {
                fatalError("Setting List properties during migrations is unsupported. Instead you can remove objects from the current List.")
            }
            super[key] = value
        }
    }

    /**
    WARNING: This is an internal initializer for Realm that must be `public`, but is not intended to
             be used directly.

    Sets a list property by passing in its name and `RLMArray` to be wrapped.

    :param: name     Name of the list property to set.
    :param: rlmArray `RLMArray` to set.
    */
    public func initalizeListPropertyWithName(name: String, rlmArray: RLMArray) {
        listProperties[name] = List<MigrationObject>(rlmArray)
    }
}

// MARK: Private Helpers

private func accessorMigrationBlock(migrationBlock: MigrationBlock) -> RLMMigrationBlock {
    return { migration, oldVersion in
        // set all accessor classes to MigrationObject
        for objectSchema in migration.oldSchema.objectSchema {
            (objectSchema as RLMObjectSchema).accessorClass = MigrationObject.self
        }
        for objectSchema in migration.newSchema.objectSchema {
            (objectSchema as RLMObjectSchema).accessorClass = MigrationObject.self
        }

        // run migration
        migrationBlock(migration: Migration(migration), oldSchemaVersion: oldVersion)
    }
}

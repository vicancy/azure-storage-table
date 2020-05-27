const azure = require('azure-storage');

function StorageAccessor(conn) {
    var retryOperations = new azure.ExponentialRetryPolicyFilter();
    const table = azure.createTableService(conn).withFilter(retryOperations);
    this.exec = function (action, ...args) {
        return new Promise((resolve, reject) => {
            let promiseHandling = (err, result) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(result);
                }
            };
            args.push(promiseHandling);
            table[action].apply(table, args);
        });
    };
    this.query = function () {
        return new azure.TableQuery();
    };
}

function ChatStorageAccessor(conn, tableName) {
    var accessor = new StorageAccessor(conn);
    this.queryChat = async function (chatKey, count) {
        query = accessor.query().where("PartitionKey eq '" + chatKey + "'").select('sentTime', 'content').top(count);
        await accessor.exec('createTableIfNotExists', tableName, t => {
            if (t) throw t;
        });
        var chats = (await accessor.exec('queryEntities', tableName, query, null)).entries.map(i => {
            return {
                time: i.sentTime['_'],
                content: i.content['_']
            };
        }).sort(i => i.time).map(i => i.content).join(',');
        return '[' + chats + ']';
    };
    this.addChat = async function (chatKey, content) {
        const date = new Date().toISOString();
        var chatEntity = {
            PartitionKey: { '_': chatKey },
            RowKey: { '_': date },
            sentTime: date,
            content: { '_': content },
        };
        await accessor.exec('insertOrReplaceEntity', tableName, chatEntity);

    }
    this.queryEntity = async function (partitionKey, columnName, count) {
        var query = accessor.query().where(`PartitionKey eq '${partitionKey}'`).select(columnName).top(count);
        return (await accessor.exec('queryEntities', tableName, query, null)).entries.map(i => i[columnName]['_']).sort();
    };
    this.updateEntity = async function (entity) {
        await accessor.exec('createTableIfNotExists', tableName);
        await accessor.exec('insertOrReplaceEntity', tableName, entity);
    };
    this.removeEntity = function (partitionKey, rowKey) {
        var entity = {
            PartitionKey: { '_': partitionKey },
            RowKey: { '_': rowKey },
        }
        return accessor.exec('deleteEntity', tableName, entity);
    };
}

function UsersAccessor(accessor, context) {
    const usersKey = '_users';

    this.user = userName => new UserAccessor(accessor, userName, context);

    this.add = function (user) {
        var userEntity = {
            PartitionKey: { '_': usersKey },
            RowKey: { '_': user },
            user: { '_': user },
            connectTime: { '_': new Date().toISOString() },
        }
        return accessor.updateEntity(userEntity);
    };

    this.remove = async function (user) {
        try {
            await accessor.removeEntity(usersKey, user);
        } catch (err) { }
    };

    this.load = function (count) {
        return accessor.queryEntity(usersKey, 'user', count);
    };
}

function UserAccessor(accessor, user, context) {

    const getChatKey = function (recipient) {
        return '_chats_user_' + [user, recipient].sort().join(';');
    };

    this.load = function (recipient, count = 20) {
        const chatKey = getChatKey(recipient);
        return accessor.queryChat(chatKey, count);
    };

    this.addChat = function (recipient, content) {
        const chatKey = getChatKey(recipient);
        return accessor.addChat(chatKey, content);
    }
}

function GroupAccessor(accessor, group, context) {
    const chatKey = '_chats_group_' + group;

    this.load = function (count = 20) {
        return accessor.queryChat(chatKey, count);
    };

    this.addChat = function (content) {
        return accessor.addChat(chatKey, content);
    };
}

function PublicChatAccessor(accessor, context) {
    const chatKey = '_chats_broadcast';
    this.load = function (count = 20) {
        return accessor.queryChat(chatKey, count);
    };

    this.addChat = function (content) {
        return accessor.addChat(chatKey, content);
    };
}

function UserGroupsChatAccessor(accessor, context) {
    const getChatKey = user => '_usergroups_' + user;

    this.load = function (user, count) {
        return accessor.queryEntity(getChatKey(user), 'group', count);
    }

    this.add = function (group, user) {
        const date = new Date().toISOString();
        var userGroup = {
            PartitionKey: { '_': getChatKey(user) },
            RowKey: { '_': group },
            user: { '_': user },
            joinedTime: { '_': date },
            group: { '_': group },
        }

        return accessor.updateEntity(userGroup);
    }

    this.remove = async function (group, user) {
        try {
            await accessor.removeEntity(getChatKey(user), group);
        } catch (err) { }
    }
}

function ChatStorageManager(accessor, context) {
    this.users = new UsersAccessor(accessor, context);
    this.public = new PublicChatAccessor(accessor, context);
    this.usergroups = new UserGroupsChatAccessor(accessor, context);
    this.group = groupName => new GroupAccessor(accessor, groupName, context);
    this.user = userName => new UserAccessor(accessor, userName, context);
}

function ChatAccessor() { }
ChatAccessor.prototype.accessor = function (conn, tableName, context) {
    var conn = process.env['AzureWebJobsStorage'];
    var tableName = process.env['ChatTableName']
    context = context || console;
    var accessor = new ChatStorageAccessor(conn, tableName);
    return new ChatStorageManager(accessor, context);
}

ChatAccessor.prototype.default = function (context) {
    var conn = process.env['AzureWebJobsStorage'];
    var tableName = process.env['ChatTableName']
    return this.accessor(conn, tableName, context);
}

var accessor = new ChatAccessor();
module.exports = accessor;

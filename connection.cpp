/*-------------------------------------------------------------------------
 *
 * connection.cpp
 *		  Connection management functions for dynamodb_fdw
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  contrib/dynamodb_fdw/connection.cpp
 *
 *-------------------------------------------------------------------------
 */
extern "C"
{
#include "postgres.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "dynamodb_fdw.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
}

#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>

static Aws::SDKOptions *aws_sdk_options;

extern "C" void
dynamodb_init()
{
	aws_sdk_options = new Aws::SDKOptions();
	Aws::InitAPI(*aws_sdk_options);
}

extern "C" void
dynamodb_shutdown()
{
	Aws::ShutdownAPI(*aws_sdk_options);
	aws_sdk_options = NULL;
}

typedef Oid ConnCacheKey;

typedef struct ConnCacheEntry
{

	ConnCacheKey key;										/* hash key (must be first) */
	Aws::DynamoDB::DynamoDBClient	   *conn;				/* connection to foreign server, or NULL */
															/* Remaining fields are invalid when conn is NULL: */
	bool								invalidated;		/* true if reconnect is pending */
	uint32								server_hashvalue;	/* hash value of foreign server OID */
	uint32								mapping_hashvalue;	/* hash value of user mapping OID */
} ConnCacheEntry;

/*
 * Connection cache (initialized on first use)
 */

static HTAB *ConnectionHash = NULL;

/* prototypes of private functions */
static void dynamodb_make_new_connection(ConnCacheEntry *entry, UserMapping *user);
static Aws::DynamoDB::DynamoDBClient *dynamodb_create_connection(ForeignServer *server, UserMapping *user);
static void dynamodb_check_conn_params(dynamodb_opt *opt);
static void dynamodb_inval_callback(Datum arg, int cacheid, uint32 hashvalue);
static Aws::DynamoDB::DynamoDBClient *dynamodb_client_open(const char *user,
														const char *password,
														const char *endpoint);
static void dynamodb_delete_client(Aws::DynamoDB::DynamoDBClient *dynamoDB_client);

/* prototypes of public functions */
extern void dynamodb_close_connection(ConnCacheEntry *entry);

/*
 * dynamodb_get_connection
 *
 * Get a connection which can be used to execute queries on
 * the remote DynamoDB with the user's authorization. A new connection
 * is established if we don't already have a suitable one.
 */
Aws::DynamoDB::DynamoDBClient *
dynamodb_get_connection(UserMapping *user)
{
	bool			found;
	ConnCacheEntry *entry;
	ConnCacheKey	key;

	/* First time through, initialize connection cache hashtable */
	if (ConnectionHash == NULL)
	{
		HASHCTL		ctl;

#if PG_VERSION_NUM < 140000
		MemSet(&ctl, 0, sizeof(ctl));
#endif
		ctl.keysize = sizeof(ConnCacheKey);
		ctl.entrysize = sizeof(ConnCacheEntry);
#if PG_VERSION_NUM < 140000
		/* allocate ConnectionHash in the cache context */
		ctl.hcxt = CacheMemoryContext;
#endif
		ConnectionHash = hash_create("dynamoDB_fdw connections", 8,
									 &ctl,
#if PG_VERSION_NUM >= 140000
									 HASH_ELEM | HASH_BLOBS);
#else
									 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
#endif

		/*
		 * Register some callback functions that manage connection cleanup.
		 * This should be done just once in each backend.
		 */
		CacheRegisterSyscacheCallback(FOREIGNSERVEROID,
									  dynamodb_inval_callback, (Datum) 0);
		CacheRegisterSyscacheCallback(USERMAPPINGOID,
									  dynamodb_inval_callback, (Datum) 0);
	}

	/* Create hash key for the entry.  Assume no pad bytes in key struct */
	key = user->umid;

	/*
	 * Find or create cached entry for requested connection.
	 */
	entry = (ConnCacheEntry *) hash_search(ConnectionHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		/*
		 * We need only clear "conn" here; remaining fields will be filled
		 * later when "conn" is set.
		 */
		entry->conn = NULL;
	}

	/*
	 * If the connection needs to be remade due to invalidation, disconnect as
	 * soon as we're out of all transactions.
	 */
	if (entry->conn != NULL && entry->invalidated)
	{
		elog(DEBUG3, "dynamodb_fdw: closing connection %p for option changes to take effect",
			 entry->conn);
		dynamodb_close_connection(entry);
	}

	/*
	 * If cache entry doesn't have a connection, we have to establish a new
	 * connection.  (If connect_dynamo_server throws an error, the cache entry
	 * will remain in a valid empty state, ie conn == NULL.)
	 */
	if (entry->conn == NULL)
		dynamodb_make_new_connection(entry, user);

	return entry->conn;
}

/*
 * Reset all transient state fields in the cached connection entry and
 * establish new connection to the remote server.
 */
static void
dynamodb_make_new_connection(ConnCacheEntry *entry, UserMapping *user)
{
	ForeignServer *server = GetForeignServer(user->serverid);

	Assert(entry->conn == NULL);

	/* Reset all transient state fields, to be sure all are clean */
	entry->invalidated = false;
	entry->server_hashvalue =
		GetSysCacheHashValue1(FOREIGNSERVEROID,
								ObjectIdGetDatum(server->serverid));
	entry->mapping_hashvalue =
		GetSysCacheHashValue1(USERMAPPINGOID,
								ObjectIdGetDatum(user->umid));

	/* Now try to make the connection */
	entry->conn = dynamodb_create_connection(server, user);

	elog(DEBUG3, "dynamodb_fdw: new dynamoDB_fdw connection %p for server \"%s\" (user mapping oid %u, userid %u)",
			entry->conn, server->servername, user->umid, user->userid);
}

/*
 * dynamodb_create_connection
 *
 * Connect to remote server using specified server and user mapping properties.
 */
static Aws::DynamoDB::DynamoDBClient *
dynamodb_create_connection(ForeignServer *server, UserMapping *user)
{
	Aws::DynamoDB::DynamoDBClient	   *volatile conn = NULL;
	dynamodb_opt *opt = dynamodb_get_options(server->serverid);

	/*
	 * Extract options from FDW objects.
	 */
	PG_TRY();
	{
		/* verify connection parameters and make connection */
		dynamodb_check_conn_params(opt);

		conn = dynamodb_client_open(opt->svr_username, opt->svr_password, opt->svr_endpoint);

		if (!conn)
			ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				errmsg("dynamodb_fdw: could not connect to DynamoDB \"%s\"",
						server->servername)));
	}
	PG_CATCH();
	{
		/* Close DynamoDB handle if we managed to create one */
		if (conn)
		{
			dynamodb_delete_client(conn);
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	return conn;
}

/*
 * dynamodb_check_conn_params
 *
 * Password is required to connect to dynamoDB.
 */
static void
dynamodb_check_conn_params(dynamodb_opt *opt)
{
	if (opt->svr_username == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
				errmsg("dynamodb_fdw: password is required"),
				errdetail("Non-superusers must provide a password in the user mapping.")));

	if (opt->svr_password == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
				errmsg("dynamodb_fdw: user is required"),
				errdetail("Non-superusers must provide an user name in the user mapping.")));
}


/*
 * dynamodb_inval_callback
 *
 * Connection invalidation callback function
 *
 * After a change to a pg_foreign_server or pg_user_mapping catalog entry,
 * close connections depending on that entry immediately if current transaction
 * has not used those connections yet. Otherwise, mark those connections as
 * invalid and then make pgfdw_xact_callback() close them at the end of current
 * transaction, since they cannot be closed in the midst of the transaction
 * using them. Closed connections will be remade at the next opportunity if
 * necessary.
 *
 * Although most cache invalidation callbacks blow away all the related stuff
 * regardless of the given hashvalue, connections are expensive enough that
 * it's worth trying to avoid that.
 *
 * NB: We could avoid unnecessary disconnection more strictly by examining
 * individual option values, but it seems too much effort for the gain.
 */
static void
dynamodb_inval_callback(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	Assert(cacheid == FOREIGNSERVEROID || cacheid == USERMAPPINGOID);

	/* ConnectionHash must exist already, if we're registered */
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		/* Ignore invalid entries */
		if (entry->conn == NULL)
			continue;

		/* hashvalue == 0 means a cache reset, must clear all state */
		if (hashvalue == 0 ||
			(cacheid == FOREIGNSERVEROID &&
			 entry->server_hashvalue == hashvalue) ||
			(cacheid == USERMAPPINGOID &&
			 entry->mapping_hashvalue == hashvalue))
			entry->invalidated = true;
	}
}

/*
 * dynamodb_client_open
 *
 * Create dynamoDB handle.
 */
static Aws::DynamoDB::DynamoDBClient*
dynamodb_client_open(const char *user, const char *password, const char *endpoint)
{
	const Aws::String access_key_id = user;
	const Aws::String secret_access_key = password;
	Aws::Client::ClientConfiguration clientConfig;
	Aws::DynamoDB::DynamoDBClient *dynamo_client;
	Aws::Auth::AWSCredentials cred(access_key_id, secret_access_key);

	clientConfig.endpointOverride = endpoint;

	dynamo_client = new Aws::DynamoDB::DynamoDBClient(cred, clientConfig);
	return dynamo_client;
}

/*
 * dynamodb_delete_client
 *
 * Delete DynamoDB client handle.
 */
static void
dynamodb_delete_client(Aws::DynamoDB::DynamoDBClient *dynamoDB_client)
{
	delete dynamoDB_client;
}

/*
 * Close any open handle for a connection cache entry.
 */
extern void
dynamodb_close_connection(ConnCacheEntry *entry)
{
	if (entry->conn != NULL)
	{
		dynamodb_delete_client(entry->conn);
		entry->conn = NULL;
	}
}

/*
 * dynamodb_report_error
 *
 * Report an error we got from the remote server.
 *
 * elevel: error level to use (typically ERROR, but might be less)
 * message: error message
 * query: the query that causes error
 *
 * Note: callers that choose not to throw ERROR for a remote error are
 * responsible for making sure that the associated ConnCacheEntry gets
 * marked with have_error = true.
 */
void
dynamodb_report_error(int elevel, const Aws::String message, char* query)
{
	int			state = ERRCODE_FDW_ERROR;

	ereport(elevel,
			(errcode(state),
			 errmsg("dynamodb_fdw: failed to execute remote SQL: %s \n   sql=%s",
					message.c_str(), query)
			 ));
}

/*
 * dynamodb_release_connection
 *
 * Release connection reference count created by calling GetConnection.
 */
void
dynamodb_release_connection(Aws::DynamoDB::DynamoDBClient *dynamoDB_client)
{
	/*
	 * Currently, we don't actually track connection references because all
	 * cleanup is managed on a transaction or subtransaction basis instead. So
	 * there's nothing to do here.
	 */
}

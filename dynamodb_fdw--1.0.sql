/* contrib/dynamodb_fdw/dynamodb_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION dynamodb_fdw" to load this file. \quit

CREATE FUNCTION dynamodb_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION dynamodb_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER dynamodb_fdw
  HANDLER dynamodb_fdw_handler
  VALIDATOR dynamodb_fdw_validator;

CREATE PROCEDURE dynamodb_create_or_replace_stub(func_type text, name_arg text, return_type regtype) AS $$
DECLARE
  proname_raw text := split_part(name_arg, '(', 1);
  proname text := ltrim(rtrim(proname_raw));
BEGIN
  IF lower(func_type) = 'aggregation' OR lower(func_type) = 'aggregate' OR lower(func_type) = 'agg' OR lower(func_type) = 'a' THEN
    DECLARE
      proargs_raw text := right(name_arg, length(name_arg) - length(proname_raw));
      proargs text := ltrim(rtrim(proargs_raw));
      proargs_types text := right(left(proargs, length(proargs) - 1), length(proargs) - 2);
      aggproargs text := format('(%s, %s)', return_type, proargs_types);
    BEGIN
      BEGIN
        EXECUTE format('
          CREATE FUNCTION %s_sfunc%s RETURNS %s IMMUTABLE AS $inner$
          BEGIN
            RAISE EXCEPTION ''stub %s_sfunc%s is called'';
            RETURN NULL;
          END $inner$ LANGUAGE plpgsql;',
	  proname, aggproargs, return_type, proname, aggproargs);
      EXCEPTION
        WHEN duplicate_function THEN
          RAISE DEBUG 'stub function for aggregation already exists (ignored)';
      END;
      BEGIN
        EXECUTE format('
          CREATE AGGREGATE %s
          (
            sfunc = %s_sfunc,
            stype = %s
          );', name_arg, proname, return_type);
      EXCEPTION
        WHEN duplicate_function THEN
          RAISE DEBUG 'stub aggregation already exists (ignored)';
        WHEN others THEN
          RAISE EXCEPTION 'stub aggregation exception';
      END;
    END;
  ELSIF lower(func_type) = 'function' OR lower(func_type) = 'func' OR lower(func_type) = 'f' THEN
    BEGIN
      EXECUTE format('
        CREATE FUNCTION %s RETURNS %s IMMUTABLE AS $inner$
        BEGIN
          RAISE EXCEPTION ''stub %s is called'';
          RETURN NULL;
        END $inner$ LANGUAGE plpgsql COST 1;',
        name_arg, return_type, name_arg);
    EXCEPTION
      WHEN duplicate_function THEN
        RAISE DEBUG 'stub already exists (ignored)';
    END;
  ELSEIF lower(func_type) = 'stable function' OR lower(func_type) = 'sfunc' OR lower(func_type) = 'sf' THEN
    BEGIN
      EXECUTE format('
        CREATE FUNCTION %s RETURNS %s STABLE AS $inner$
        BEGIN
          RAISE EXCEPTION ''stub %s is called'';
          RETURN NULL;
        END $inner$ LANGUAGE plpgsql COST 1;',
        name_arg, return_type, name_arg);
    EXCEPTION
      WHEN duplicate_function THEN
        RAISE DEBUG 'stub already exists (ignored)';
    END;
  ELSEIF lower(func_type) = 'volatile function' OR lower(func_type) = 'vfunc' OR lower(func_type) = 'vf' THEN
    BEGIN
      EXECUTE format('
        CREATE FUNCTION %s RETURNS %s VOLATILE AS $inner$
        BEGIN
          RAISE EXCEPTION ''stub %s is called'';
          RETURN NULL;
        END $inner$ LANGUAGE plpgsql COST 1;',
        name_arg, return_type, name_arg);
    EXCEPTION
      WHEN duplicate_function THEN
        RAISE DEBUG 'stub already exists (ignored)';
    END;
  ELSE
    RAISE EXCEPTION 'not supported function type %', func_type;
    BEGIN
      EXECUTE format('
        CREATE FUNCTION %s_sfunc RETURNS %s AS $inner$
        BEGIN
          RAISE EXCEPTION ''stub %s is called'';
          RETURN NULL;
       END $inner$ LANGUAGE plpgsql COST 1;',
        name_arg, return_type, name_arg);
    EXCEPTION
      WHEN duplicate_function THEN
        RAISE DEBUG 'stub already exists (ignored)';
    END;
  END IF;
END
$$ LANGUAGE plpgsql;

CALL dynamodb_create_or_replace_stub('f', 'size(anyelement)', 'bigint');
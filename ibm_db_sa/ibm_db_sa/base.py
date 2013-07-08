# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2008, 2013.                                |
# +--------------------------------------------------------------------------+
# | This module complies with SQLAlchemy 0.8 and is                          |
# | Licensed under the Apache License, Version 2.0 (the "License");          |
# | you may not use this file except in compliance with the License.         |
# | You may obtain a copy of the License at                                  |
# | http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable |
# | law or agreed to in writing, software distributed under the License is   |
# | distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY |
# | KIND, either express or implied. See the License for the specific        |
# | language governing permissions and limitations under the License.        |
# +--------------------------------------------------------------------------+
# | Authors: Alex Pitigoi, Abhigyan Agrawal                                  |
# | Contributors: Jaimy Azle, Mike Bayer                                     |
# | Version: 0.3.x                                                           |
# +--------------------------------------------------------------------------+
"""Support for IBM DB2 database

"""
import datetime
from sqlalchemy import exc
from sqlalchemy import types as sa_types
from sqlalchemy import schema as sa_schema
from sqlalchemy import sql
from sqlalchemy import util
from sqlalchemy.sql import compiler
from sqlalchemy.engine import default

from . import reflection as ibm_reflection

from sqlalchemy.types import BLOB, CHAR, CLOB, DATE, DATETIME, INTEGER,\
    SMALLINT, BIGINT, DECIMAL, NUMERIC, REAL, TIME, TIMESTAMP,\
    VARCHAR


# as documented from:
# http://publib.boulder.ibm.com/infocenter/db2luw/v9/index.jsp?topic=/com.ibm.db2.udb.doc/admin/r0001095.htm
RESERVED_WORDS = set(
   ['activate', 'disallow', 'locale', 'result', 'add', 'disconnect', 'localtime',
    'result_set_locator', 'after', 'distinct', 'localtimestamp', 'return', 'alias',
    'do', 'locator', 'returns', 'all', 'double', 'locators', 'revoke', 'allocate', 'drop',
    'lock', 'right', 'allow', 'dssize', 'lockmax', 'rollback', 'alter', 'dynamic',
    'locksize', 'routine', 'and', 'each', 'long', 'row', 'any', 'editproc', 'loop',
    'row_number', 'as', 'else', 'maintained', 'rownumber', 'asensitive', 'elseif',
    'materialized', 'rows', 'associate', 'enable', 'maxvalue', 'rowset', 'asutime',
    'encoding', 'microsecond', 'rrn', 'at', 'encryption', 'microseconds', 'run',
    'attributes', 'end', 'minute', 'savepoint', 'audit', 'end-exec', 'minutes', 'schema',
    'authorization', 'ending', 'minvalue', 'scratchpad', 'aux', 'erase', 'mode', 'scroll',
    'auxiliary', 'escape', 'modifies', 'search', 'before', 'every', 'month', 'second',
    'begin', 'except', 'months', 'seconds', 'between', 'exception', 'new', 'secqty',
    'binary', 'excluding', 'new_table', 'security', 'bufferpool', 'exclusive',
    'nextval', 'select', 'by', 'execute', 'no', 'sensitive', 'cache', 'exists', 'nocache',
    'sequence', 'call', 'exit', 'nocycle', 'session', 'called', 'explain', 'nodename',
    'session_user', 'capture', 'external', 'nodenumber', 'set', 'cardinality',
    'extract', 'nomaxvalue', 'signal', 'cascaded', 'fenced', 'nominvalue', 'simple',
    'case', 'fetch', 'none', 'some', 'cast', 'fieldproc', 'noorder', 'source', 'ccsid',
    'file', 'normalized', 'specific', 'char', 'final', 'not', 'sql', 'character', 'for',
    'null', 'sqlid', 'check', 'foreign', 'nulls', 'stacked', 'close', 'free', 'numparts',
    'standard', 'cluster', 'from', 'obid', 'start', 'collection', 'full', 'of', 'starting',
    'collid', 'function', 'old', 'statement', 'column', 'general', 'old_table', 'static',
    'comment', 'generated', 'on', 'stay', 'commit', 'get', 'open', 'stogroup', 'concat',
    'global', 'optimization', 'stores', 'condition', 'go', 'optimize', 'style', 'connect',
    'goto', 'option', 'substring', 'connection', 'grant', 'or', 'summary', 'constraint',
    'graphic', 'order', 'synonym', 'contains', 'group', 'out', 'sysfun', 'continue',
    'handler', 'outer', 'sysibm', 'count', 'hash', 'over', 'sysproc', 'count_big',
    'hashed_value', 'overriding', 'system', 'create', 'having', 'package',
    'system_user', 'cross', 'hint', 'padded', 'table', 'current', 'hold', 'pagesize',
    'tablespace', 'current_date', 'hour', 'parameter', 'then', 'current_lc_ctype',
    'hours', 'part', 'time', 'current_path', 'identity', 'partition', 'timestamp',
    'current_schema', 'if', 'partitioned', 'to', 'current_server', 'immediate',
    'partitioning', 'transaction', 'current_time', 'in', 'partitions', 'trigger',
    'current_timestamp', 'including', 'password', 'trim', 'current_timezone',
    'inclusive', 'path', 'type', 'current_user', 'increment', 'piecesize', 'undo',
    'cursor', 'index', 'plan', 'union', 'cycle', 'indicator', 'position', 'unique', 'data',
    'inherit', 'precision', 'until', 'database', 'inner', 'prepare', 'update',
    'datapartitionname', 'inout', 'prevval', 'usage', 'datapartitionnum',
    'insensitive', 'primary', 'user', 'date', 'insert', 'priqty', 'using', 'day',
    'integrity', 'privileges', 'validproc', 'days', 'intersect', 'procedure', 'value',
    'db2general', 'into', 'program', 'values', 'db2genrl', 'is', 'psid', 'variable',
    'db2sql', 'isobid', 'query', 'variant', 'dbinfo', 'isolation', 'queryno', 'vcat',
    'dbpartitionname', 'iterate', 'range', 'version', 'dbpartitionnum', 'jar', 'rank',
    'view', 'deallocate', 'java', 'read', 'volatile', 'declare', 'join', 'reads', 'volumes',
    'default', 'key', 'recovery', 'when', 'defaults', 'label', 'references', 'whenever',
    'definition', 'language', 'referencing', 'where', 'delete', 'lateral', 'refresh',
    'while', 'dense_rank', 'lc_ctype', 'release', 'with', 'denserank', 'leave', 'rename',
    'without', 'describe', 'left', 'repeat', 'wlm', 'descriptor', 'like', 'reset', 'write',
    'deterministic', 'linktype', 'resignal', 'xmlelement', 'diagnostics', 'local',
    'restart', 'year', 'disable', 'localdate', 'restrict', 'years', '', 'abs', 'grouping',
    'regr_intercept', 'are', 'int', 'regr_r2', 'array', 'integer', 'regr_slope',
    'asymmetric', 'intersection', 'regr_sxx', 'atomic', 'interval', 'regr_sxy', 'avg',
    'large', 'regr_syy', 'bigint', 'leading', 'rollup', 'blob', 'ln', 'scope', 'boolean',
    'lower', 'similar', 'both', 'match', 'smallint', 'ceil', 'max', 'specifictype',
    'ceiling', 'member', 'sqlexception', 'char_length', 'merge', 'sqlstate',
    'character_length', 'method', 'sqlwarning', 'clob', 'min', 'sqrt', 'coalesce', 'mod',
    'stddev_pop', 'collate', 'module', 'stddev_samp', 'collect', 'multiset',
    'submultiset', 'convert', 'national', 'sum', 'corr', 'natural', 'symmetric',
    'corresponding', 'nchar', 'tablesample', 'covar_pop', 'nclob', 'timezone_hour',
    'covar_samp', 'normalize', 'timezone_minute', 'cube', 'nullif', 'trailing',
    'cume_dist', 'numeric', 'translate', 'current_default_transform_group',
    'octet_length', 'translation', 'current_role', 'only', 'treat',
    'current_transform_group_for_type', 'overlaps', 'true', 'dec', 'overlay',
    'uescape', 'decimal', 'percent_rank', 'unknown', 'deref', 'percentile_cont',
    'unnest', 'element', 'percentile_disc', 'upper', 'exec', 'power', 'var_pop', 'exp',
    'real', 'var_samp', 'false', 'recursive', 'varchar', 'filter', 'ref', 'varying',
    'float', 'regr_avgx', 'width_bucket', 'floor', 'regr_avgy', 'window', 'fusion',
    'regr_count', 'within'])


class _IBM_Date(sa_types.Date):

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            if isinstance(value, datetime.datetime):
                value = datetime.date(value.year, value.month, value.day)
            return value
        return process

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            if isinstance(value, datetime.datetime):
                value = datetime.date(value.year, value.month, value.day)
            return str(value)
        return process

class DOUBLE(sa_types.Numeric):
    __visit_name__ = 'DOUBLE'

class LONGVARCHAR(sa_types.VARCHAR):
    __visit_name_ = 'LONGVARCHAR'

class DBCLOB(sa_types.CLOB):
    __visit_name__ = "DBCLOB"

class GRAPHIC(sa_types.CHAR):
    __visit_name__ = "GRAPHIC"

class VARGRAPHIC(sa_types.Unicode):
    __visit_name__ = "VARGRAPHIC"


class LONGVARGRAPHIC(sa_types.UnicodeText):
    __visit_name__ = "LONGVARGRAPHIC"

class XML(sa_types.Text):
    __visit_name__ = "XML"

colspecs = {
    sa_types.Date: _IBM_Date,
# really ?
#    sa_types.Unicode: DB2VARGRAPHIC
}

ischema_names = {
    'BLOB': BLOB,
    'CHAR': CHAR,
    'CHARACTER': CHAR,
    'CLOB': CLOB,
    'DATE': DATE,
    'DATETIME': DATETIME,
    'INTEGER': INTEGER,
    'SMALLINT': SMALLINT,
    'BIGINT': BIGINT,
    'DECIMAL': DECIMAL,
    'NUMERIC': NUMERIC,
    'REAL': REAL,
    'DOUBLE': DOUBLE,
    'TIME': TIME,
    'TIMESTAMP': TIMESTAMP,
    'VARCHAR': VARCHAR,
    'LONGVARCHAR': LONGVARCHAR,
    'XML': XML,
    'GRAPHIC': GRAPHIC,
    'VARGRAPHIC': VARGRAPHIC,
    'LONGVARGRAPHIC': LONGVARGRAPHIC,
    'DBCLOB': DBCLOB
}


class DB2TypeCompiler(compiler.GenericTypeCompiler):


    def visit_TIMESTAMP(self, type_):
        return "TIMESTAMP"

    def visit_DATE(self, type_):
        return "DATE"

    def visit_TIME(self, type_):
        return "TIME"

    def visit_DATETIME(self, type_):
        return self.visit_TIMESTAMP(type_)

    def visit_SMALLINT(self, type_):
        return "SMALLINT"

    def visit_INT(self, type_):
        return "INT"

    def visit_BIGINT(self, type_):
        return "BIGINT"

    def visit_REAL(self, type_):
        return "REAL"

    def visit_XML(self, type_):
        return "XML"

    def visit_CLOB(self, type_):
        return "CLOB"

    def visit_BLOB(self, type_):
        return "BLOB(1M)" if type_.length in (None, 0) else \
                "BLOB(%(length)s)" % {'length': type_.length}

    def visit_DBCLOB(self, type_):
        return "DBCLOB(1M)" if type_.length in (None, 0) else \
                "DBCLOB(%(length)s)" % {'length': type_.length}

    def visit_VARCHAR(self, type_):
        return "VARCHAR(%(length)s)" % {'length': type_.length}

    def visit_LONGVARCHAR(self, type_):
        return "LONG VARCHAR"

    def visit_VARGRAPHIC(self, type_):
        return "VARGRAPHIC(%(length)s)" % {'length': type_.length}

    def visit_LONGVARGRAPHIC(self, type_):
        return "LONG VARGRAPHIC"

    def visit_CHAR(self, type_):
        return "CHAR" if type_.length in (None, 0) else \
                "CHAR(%(length)s)" % {'length': type_.length}

    def visit_GRAPHIC(self, type_):
        return "GRAPHIC" if type_.length in (None, 0) else \
                "GRAPHIC(%(length)s)" % {'length': type_.length}

    def visit_DECIMAL(self, type_):
        if not type_.precision:
            return "DECIMAL(31, 0)"
        elif not type_.scale:
            return "DECIMAL(%(precision)s, 0)" % {'precision': type_.precision}
        else:
            return "DECIMAL(%(precision)s, %(scale)s)" % {
                            'precision': type_.precision, 'scale': type_.scale}


    def visit_numeric(self, type_):
        return self.visit_DECIMAL(type_)

    def visit_datetime(self, type_):
        return self.visit_TIMESTAMP(type_)

    def visit_date(self, type_):
        return self.visit_DATE(type_)

    def visit_time(self, type_):
        return self.visit_TIME(type_)

    def visit_integer(self, type_):
        return self.visit_INT(type_)

    def visit_boolean(self, type_):
        return self.visit_SMALLINT(type_)

    def visit_float(self, type_):
        return self.visit_REAL(type_)

    def visit_unicode(self, type_):
        return self.visit_VARGRAPHIC(type_)

    def visit_unicode_text(self, type_):
        return self.visit_LONGVARGRAPHIC(type_)

    def visit_string(self, type_):
        return self.visit_VARCHAR(type_)

    def visit_TEXT(self, type_):
        return self.visit_CLOB(type_)

    def visit_large_binary(self, type_):
        return self.visit_BLOB(type_)


class DB2Compiler(compiler.SQLCompiler):


    def visit_now_func(self, fn, **kw):
        return "CURRENT_TIMESTAMP"

    def visit_mod_binary(self, binary, operator, **kw):
        return "mod(%s, %s)" % (self.process(binary.left),
                                                self.process(binary.right))

    def visit_true(self, expr, **kw):
        return '1'

    def visit_false(self, expr, **kw):
        return '0'

    def limit_clause(self, select):
        if select._offset is None and select._limit is not None:
            return " FETCH FIRST %s ROWS ONLY" % select._limit
        else:
            return ""

    def visit_select(self, select, **kwargs):
        """Look for ``LIMIT`` and OFFSET in a select statement, and if
        so tries to wrap it in a subquery with ``row_number()`` criterion.

        """
        if select._offset and not getattr(select, '_db2_visit', None):
            # to use ROW_NUMBER(), an ORDER BY is required.
            if not select._order_by_clause.clauses:
                raise exc.CompileError('DB2 requires an order_by when '
                                              'using an offset.')

            _offset = select._offset
            _limit = select._limit
            _order_by_clauses = select._order_by_clause.clauses
            select = select._generate()
            select._db2_visit = True
            select = select.column(
                 sql.func.ROW_NUMBER().over(order_by=_order_by_clauses)
                     .label("db2_rn")
                                   ).order_by(None).alias()

            db2_rn = sql.column('db2_rn')
            limitselect = sql.select([c for c in select.c if
                                        c.key != 'db2_rn'])
            limitselect.append_whereclause(db2_rn > _offset)
            if _limit is not None:
                limitselect.append_whereclause(db2_rn <= (_limit + _offset))
            return self.process(limitselect, iswrapper=True, **kwargs)
        else:
            return compiler.SQLCompiler.visit_select(self, select, **kwargs)

    def visit_sequence(self, sequence):
        return "NEXT VALUE FOR %s" % sequence.name

    def default_from(self):
        # DB2 uses SYSIBM.SYSDUMMY1 table for row count
        return  " FROM SYSIBM.SYSDUMMY1"

    #def visit_function(self, func, result_map=None, **kwargs):
    # TODO: this is wrong but need to know what DB2 is expecting here
    #    if func.name.upper() == "LENGTH":
    #        return "LENGTH('%s')" % func.compile().params[func.name + '_1']
    #   else:
    #        return compiler.SQLCompiler.visit_function(self, func, **kwargs)

    def get_select_precolumns(self, select):
        if isinstance(select._distinct, basestring):
            return select._distinct.upper() + " "
        elif select._distinct:
            return "DISTINCT "
        else:
            return ""

    def visit_join(self, join, asfrom=False, **kwargs):
        # NOTE: this is the same method as that used in mysql/base.py
        # to render INNER JOIN
        return ''.join(
            (self.process(join.left, asfrom=True, **kwargs),
             (join.isouter and " LEFT OUTER JOIN " or " INNER JOIN "),
             self.process(join.right, asfrom=True, **kwargs),
             " ON ",
             self.process(join.onclause, **kwargs)))

    def visit_savepoint(self, savepoint_stmt):
        return ("SAVEPOINT %s ON ROLLBACK RETAIN CURSORS"
            % self.preparer.format_savepoint(savepoint_stmt))


class DB2DDLCompiler(compiler.DDLCompiler):

    def get_column_specification(self, column, **kw):
        col_spec = [self.preparer.format_column(column)]
        col_spec.append(self.dialect.type_compiler.process(column.type))


        # column-options: "NOT NULL"
        if not column.nullable or column.primary_key:
            col_spec.append('NOT NULL')

        # default-clause:
        default = self.get_column_default_string(column)
        if default is not None:
            col_spec.append('WITH DEFAULT')
            col_spec.append(default)

        if column is column.table._autoincrement_column:
            col_spec.append('GENERATED BY DEFAULT')
            col_spec.append('AS IDENTITY')
            col_spec.append('(START WITH 1)')

        column_spec = ' '.join(col_spec)
        return column_spec

    def define_constraint_cascades(self, constraint):
        text = ""
        if constraint.ondelete is not None:
            text += " ON DELETE %s" % constraint.ondelete

        if constraint.onupdate is not None:
            util.warn(
                "DB2 does not support UPDATE CASCADE for foreign keys.")

        return text

    def visit_drop_index(self, drop, **kw):
        return "\nDROP INDEX %s" % (
                        self.preparer.quote(
                                    self._index_identifier(drop.element.name),
                                    drop.element.quote)
                        )

    def visit_drop_constraint(self, drop, **kw):
        constraint = drop.element
        if isinstance(constraint, sa_schema.PrimaryKeyConstraint):
            qual = "PRIMARY KEY "
            const = ""
        else:
            const = self.preparer.format_constraint(constraint)
            if isinstance(constraint, sa_schema.ForeignKeyConstraint):
                qual = "FOREIGN KEY "
            elif isinstance(constraint, sa_schema.UniqueConstraint):
                qual = "UNIQUE "
            elif isinstance(constraint, sa_schema.CheckConstraint):
                qual = "CHECK "
            # Not sure if necessary. Do we have other constraint type?
            else:
                qual = "CONSTRAINT "
        return "ALTER TABLE %s DROP %s%s" % \
                                (self.preparer.format_table(constraint.table),
                                qual, const)


class DB2IdentifierPreparer(compiler.IdentifierPreparer):

    reserved_words = RESERVED_WORDS
    illegal_initial_characters = set(xrange(0, 10)).union(["_", "$"])


class DB2ExecutionContext(default.DefaultExecutionContext):
    def fire_sequence(self, seq, type_):
        return self._execute_scalar("SELECT NEXTVAL FOR " +
                    self.dialect.identifier_preparer.format_sequence(seq) +
                    " FROM SYSIBM.SYSDUMMY1", type_)


class _SelectLastRowIDMixin(object):
    _select_lastrowid = False
    _lastrowid = None


    def getlastrowid(self):
        return self._lastrowid

    def pre_exec(self):
        if self.isinsert:
            tbl = self.compiled.statement.table
            seq_column = tbl._autoincrement_column
            insert_has_sequence = seq_column is not None

            self._select_lastrowid = insert_has_sequence and \
                                        not self.compiled.returning and \
                                        not self.compiled.inline

    def post_exec(self):
        conn = self.root_connection
        if self._select_lastrowid:
            conn._cursor_execute(self.cursor,
                    "SELECT IDENTITY_VAL_LOCAL() FROM SYSIBM.SYSDUMMY1",
                    (), self)
            row = self.cursor.fetchall()[0]
            if row[0] is not None:
                self._lastrowid = int(row[0])


class DB2Dialect(default.DefaultDialect):

    name = 'db2'
    max_identifier_length = 128
    encoding = 'utf-8'
    default_paramstyle = 'named'
    colspecs = colspecs
    ischema_names = ischema_names
    supports_unicode_statements = False
    supports_unicode_binds = False
    returns_unicode_strings = False
    postfetch_lastrowid = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_native_decimal = True
    preexecute_sequences = False
    supports_alter = True
    supports_sequences = True
    sequences_optional = True

    requires_name_normalize = True

    supports_default_values = False
    supports_empty_insert = False

    statement_compiler = DB2Compiler
    ddl_compiler = DB2DDLCompiler
    type_compiler = DB2TypeCompiler
    preparer = DB2IdentifierPreparer
    execution_ctx_cls = DB2ExecutionContext

    _reflector_cls = ibm_reflection.DB2Reflector

    def __init__(self, **kw):
        super(DB2Dialect, self).__init__(**kw)

        self._reflector = self._reflector_cls(self)

    # reflection: these all defer to an BaseDB2Reflector
    # object which selects between DB2 and AS/400 schemas

    def normalize_name(self, name):
        return self._reflector.normalize_name(name)

    def denormalize_name(self, name):
        return self._reflector.denormalize_name(name)

    def _get_default_schema_name(self, connection):
        return self._reflector._get_default_schema_name(connection)

    def has_table(self, connection, table_name, schema=None):
        return self._reflector.has_table(connection, table_name, schema=schema)

    def has_sequence(self, connection, sequence_name, schema=None):
        return self._reflector.has_sequence(connection, sequence_name,
                        schema=schema)

    def get_schema_names(self, connection, **kw):
        return self._reflector.get_schema_names(connection, **kw)


    def get_table_names(self, connection, schema=None, **kw):
        return self._reflector.get_table_names(connection, schema=schema, **kw)

    def get_view_names(self, connection, schema=None, **kw):
        return self._reflector.get_view_names(connection, schema=schema, **kw)

    def get_view_definition(self, connection, viewname, schema=None, **kw):
        return self._reflector.get_view_definition(
                                connection, viewname, schema=schema, **kw)

    def get_columns(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_columns(
                                connection, table_name, schema=schema, **kw)

    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_primary_keys(
                                connection, table_name, schema=schema, **kw)

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_foreign_keys(
                                connection, table_name, schema=schema, **kw)

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return self._reflector.get_indexes(
                                connection, table_name, schema=schema, **kw)


# legacy naming
IBM_DBCompiler = DB2Compiler
IBM_DBDDLCompiler = DB2DDLCompiler
IBM_DBIdentifierPreparer = DB2IdentifierPreparer
IBM_DBExecutionContext = DB2ExecutionContext
IBM_DBDialect = DB2Dialect

dialect = DB2Dialect

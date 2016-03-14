import six

from sqlalchemy.sql import ColumnElement
from sqlalchemy.orm import Mapper, class_mapper
from sqlalchemy.orm.exc import UnmappedClassError
from sqlalchemy.dialects import postgresql

__version__ = '0.1.0'

def copy_to(select, engine, fp, **flags):
    """Export a query or select to a file. For flags, see the PostgreSQL
    documentation at http://www.postgresql.org/docs/9.5/static/sql-copy.html.

    Examples: ::
        select = MyTable.select()
        with open('/path/to/file.tsv', 'w') as fp:
            export(select, engine, fp)

        query = session.query(MyModel)
        with open('/path/to/file/csv', 'w') as fp:
            export(query, engine, fp, format='csv', null='.')

    :param select: SQLAlchemy query or select
    :param engine: SQLAlchemy engine
    :param fp: File pointer, using write mode
    """
    dialect = postgresql.dialect()
    statement = getattr(select, 'statement', select)
    compiled = statement.compile(dialect=dialect)
    conn = engine.raw_connection()
    cursor = conn.cursor()
    query = cursor.mogrify(compiled.string, compiled.params).decode()
    formatted_flags = '({})'.format(format_flags(flags)) if flags else ''
    copy = 'COPY ({}) TO STDOUT {}'.format(query, formatted_flags)
    cursor.copy_expert(copy, fp)
    conn.close()

def format_flags(flags):
    return ', '.join(
        '{} {}'.format(key.upper(), format_flag(value))
        for key, value in flags.items()
    )

def format_flag(value):
    return (
        six.text_type(value).upper()
        if isinstance(value, bool)
        else repr(value)
    )

def query_entities(query):
    return sum(
        [desc_entities(desc) for desc in query.column_descriptions],
        []
    )

def desc_entities(desc):
    expr, name = desc['expr'], desc['name']
    if isinstance(expr, Mapper):
        return mapper_entities(expr)
    elif is_model(expr):
        return mapper_entities(expr.__mapper__)
    elif isinstance(expr, ColumnElement):
        return expr.label(name)
    else:
        raise ValueError()

def mapper_entities(mapper):
    model = mapper.class_
    return [
        getattr(model, prop.key).label(prop.key)
        for prop in mapper.column_attrs
    ]

def is_model(class_):
    try:
        class_mapper(class_)
        return True
    except UnmappedClassError:
        return False

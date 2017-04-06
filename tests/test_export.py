import io

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

from postgres_copy import copy_to, copy_from, relabel_query

Base = declarative_base()
engine = sa.create_engine('postgresql:///copy-test')
connection_types = {
    'engine': lambda session: session.connection().engine,
    'connection': lambda session: session.connection(),
    'raw_connection': lambda session: session.connection().connection,
}

class Album(Base):
    __tablename__ = 'album'
    id = sa.Column('aid', sa.Integer, primary_key=True)
    name = sa.Column(sa.Text)

@pytest.yield_fixture
def session():
    Session = sa.orm.sessionmaker(bind=engine)
    Base.metadata.create_all(bind=engine)
    s = Session()
    try:
        yield s
    finally:
        s.close()

@pytest.yield_fixture
def objects(session):
    rows = [
        Album(name='A Night at the Opera'),
        Album(name='A Day at the Races'),
        Album(name='Jazz'),
    ]
    for row in rows:
        session.add(row)
    session.commit()
    try:
        yield rows
    finally:
        engine.execute(Album.__table__.delete())

@pytest.mark.parametrize("conn_type", connection_types.values())
class TestCopyTo:

    def test_copy_query(self, session, objects, conn_type):
        sio = io.StringIO()
        copy_to(session.query(Album), sio, conn_type(session))
        lines = sio.getvalue().strip().split('\n')
        assert len(lines) == 3
        assert lines[0].split('\t') == [str(objects[0].id), objects[0].name]

    def test_copy_table(self, session, objects, conn_type):
        sio = io.StringIO()
        copy_to(Album.__table__.select(), sio, conn_type(session))
        lines = sio.getvalue().strip().split('\n')
        assert len(lines) == 3
        assert lines[0].split('\t') == [str(objects[0].id), objects[0].name]

    def test_copy_csv(self, session, objects, conn_type):
        sio = io.StringIO()
        flags = {'format': 'csv', 'header': True}
        copy_to(session.query(Album), sio, conn_type(session), **flags)
        lines = sio.getvalue().strip().split('\n')
        assert len(lines) == 4
        assert lines[0].split(',') == ['aid', 'name']
        assert lines[1].split(',') == [str(objects[0].id), objects[0].name]

@pytest.mark.parametrize("conn_type", connection_types.values())
class TestCopyRename:

    def test_rename_model(self, session, objects, conn_type):
        sio = io.StringIO()
        flags = {'format': 'csv', 'header': True}
        query = relabel_query(session.query(Album))
        copy_to(query, sio, conn_type(session), **flags)
        lines = sio.getvalue().strip().split('\n')
        assert len(lines) == 4
        assert lines[0].split(',') == ['id', 'name']
        assert lines[1].split(',') == [str(objects[0].id), objects[0].name]

    def test_rename_columns(self, session, objects, conn_type):
        sio = io.StringIO()
        flags = {'format': 'csv', 'header': True}
        query = relabel_query(session.query(Album.id, Album.name.label('title')))
        copy_to(query, sio, conn_type(session), **flags)
        lines = sio.getvalue().strip().split('\n')
        assert len(lines) == 4
        assert lines[0].split(',') == ['id', 'title']
        assert lines[1].split(',') == [str(objects[0].id), objects[0].name]

@pytest.mark.parametrize("conn_type", connection_types.values())
class TestCopyFrom:

    def test_copy_model(self, session, objects, conn_type):
        sio = io.StringIO()
        sio.write(u'\t'.join(['4', 'The Works']))
        sio.seek(0)
        copy_from(sio, Album, conn_type(session))
        assert session.query(Album).count() == len(objects) + 1
        row = session.query(Album).filter_by(id=4).first()
        assert row.id == 4
        assert row.name == 'The Works'

    def test_copy_table(self, session, objects, conn_type):
        sio = io.StringIO()
        sio.write(u'\t'.join(['4', 'The Works']))
        sio.seek(0)
        copy_from(sio, Album.__table__, conn_type(session))
        assert session.query(Album).count() == len(objects) + 1
        row = session.query(Album).filter_by(id=4).first()
        assert row.id == 4
        assert row.name == 'The Works'

    def test_copy_csv(self, session, objects, conn_type):
        sio = io.StringIO()
        sio.write(
            u'\n'.join([
                ','.join(['aid', 'name']),
                ','.join(['4', 'The Works'])
            ])
        )
        sio.seek(0)
        flags = {'format': 'csv', 'header': True}
        copy_from(sio, Album, conn_type(session), **flags)
        assert session.query(Album).count() == len(objects) + 1
        row = session.query(Album).filter_by(id=4).first()
        assert row.id == 4
        assert row.name == 'The Works'

    def test_copy_columns(self, session, objects, conn_type):
        sio = io.StringIO()
        sio.write(u'\t'.join(['The Works', '4']))
        sio.seek(0)
        copy_from(sio, Album, conn_type(session), columns=('name', 'aid'))
        assert session.query(Album).count() == len(objects) + 1
        row = session.query(Album).filter_by(id=4).first()
        assert row.id == 4
        assert row.name == 'The Works'

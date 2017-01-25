import io

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

from postgres_copy import copy_to, copy_from, relabel_query

Base = declarative_base()
engine = sa.create_engine('postgresql:///copy-test')

class Album(Base):
    __tablename__ = 'album'
    id = sa.Column('aid', sa.Integer, primary_key=True)
    name = sa.Column(sa.Text)

@pytest.fixture()
def session():
    Session = sa.orm.sessionmaker(bind=engine)
    Base.metadata.create_all(bind=engine)
    return Session()

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

class TestCopyTo(object):

    def test_copy_query(self, session, objects):
        sio = io.StringIO()
        copy_to(session.query(Album), sio, session.connection().engine)
        lines = sio.getvalue().strip().split('\n')
        assert len(lines) == 3
        assert lines[0].split('\t') == [str(objects[0].id), objects[0].name]

    def test_copy_table(self, session, objects):
        sio = io.StringIO()
        copy_to(Album.__table__.select(), sio, session.connection().engine)
        lines = sio.getvalue().strip().split('\n')
        assert len(lines) == 3
        assert lines[0].split('\t') == [str(objects[0].id), objects[0].name]

    def test_copy_csv(self, session, objects):
        sio = io.StringIO()
        flags = {'format': 'csv', 'header': True}
        copy_to(session.query(Album), sio, session.connection().engine, **flags)
        lines = sio.getvalue().strip().split('\n')
        assert len(lines) == 4
        assert lines[0].split(',') == ['aid', 'name']
        assert lines[1].split(',') == [str(objects[0].id), objects[0].name]

class TestCopyRename(object):

    def test_rename_model(self, session, objects):
        sio = io.StringIO()
        flags = {'format': 'csv', 'header': True}
        query = relabel_query(session.query(Album))
        copy_to(query, sio, session.connection().engine, **flags)
        lines = sio.getvalue().strip().split('\n')
        assert len(lines) == 4
        assert lines[0].split(',') == ['id', 'name']
        assert lines[1].split(',') == [str(objects[0].id), objects[0].name]

    def test_rename_columns(self, session, objects):
        sio = io.StringIO()
        flags = {'format': 'csv', 'header': True}
        query = relabel_query(session.query(Album.id, Album.name.label('title')))
        copy_to(query, sio, session.connection().engine, **flags)
        lines = sio.getvalue().strip().split('\n')
        assert len(lines) == 4
        assert lines[0].split(',') == ['id', 'title']
        assert lines[1].split(',') == [str(objects[0].id), objects[0].name]

class TestCopyFrom(object):

    def test_copy_model(self, session, objects):
        sio = io.StringIO()
        sio.write(u'\t'.join(['4', 'The Works']))
        sio.seek(0)
        copy_from(sio, Album, session.connection().engine)
        assert session.query(Album).count() == len(objects) + 1
        row = session.query(Album).filter_by(id=4).first()
        assert row.id == 4
        assert row.name == 'The Works'

    def test_copy_table(self, session, objects):
        sio = io.StringIO()
        sio.write(u'\t'.join(['4', 'The Works']))
        sio.seek(0)
        copy_from(sio, Album.__table__, session.connection().engine)
        assert session.query(Album).count() == len(objects) + 1
        row = session.query(Album).filter_by(id=4).first()
        assert row.id == 4
        assert row.name == 'The Works'

    def test_copy_csv(self, session, objects):
        sio = io.StringIO()
        sio.write(
            u'\n'.join([
                ','.join(['aid', 'name']),
                ','.join(['4', 'The Works'])
            ])
        )
        sio.seek(0)
        flags = {'format': 'csv', 'header': True}
        copy_from(sio, Album, session.connection().engine, **flags)
        assert session.query(Album).count() == len(objects) + 1
        row = session.query(Album).filter_by(id=4).first()
        assert row.id == 4
        assert row.name == 'The Works'

    def test_copy_columns(self, session, objects):
        sio = io.StringIO()
        sio.write(u'\t'.join(['The Works', '4']))
        sio.seek(0)
        copy_from(sio, Album, session.connection().engine, columns=('name', 'id', ))
        assert session.query(Album).count() == len(objects) + 1
        row = session.query(Album).filter_by(id=4).first()
        assert row.id == 4
        assert row.name == 'The Works'

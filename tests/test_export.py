import io

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

from postgres_copy import copy_to

Base = declarative_base()
engine = sa.create_engine('postgresql:///export-test')

class Album(Base):
    __tablename__ = 'album'
    id = sa.Column(sa.Integer, primary_key=True)
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
        for row in rows:
            session.delete(row)
        session.commit()

class TestExport:

    def test_export_query(self, session, objects):
        sio = io.StringIO()
        copy_to(session.query(Album), session.connection().engine, sio)
        lines = sio.getvalue().strip().split('\n')
        assert len(lines) == 3
        assert lines[0].split('\t') == [str(objects[0].id), objects[0].name]

    def test_export_table(self, session, objects):
        sio = io.StringIO()
        copy_to(Album.__table__.select(), session.connection().engine, sio)
        lines = sio.getvalue().strip().split('\n')
        assert len(lines) == 3
        assert lines[0].split('\t') == [str(objects[0].id), objects[0].name]

    def test_export_csv(self, session, objects):
        sio = io.StringIO()
        flags = {'format': 'csv', 'header': True}
        copy_to(session.query(Album), session.connection().engine, sio, **flags)
        lines = sio.getvalue().strip().split('\n')
        assert len(lines) == 4
        assert lines[0].split(',') == ['id', 'name']
        assert lines[1].split(',') == [str(objects[0].id), objects[0].name]

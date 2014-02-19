#!/usr/bin/python
"""
Organize PDFs into category folders
"""
import os, sys, re, urllib
try:
  import sqlite3
except:
  from pysqlite2 import dbapi2 as sqlite3

class MendeleyDB:
  """
  Mendeley database class for accessing the database
  """
  def __init__(self, database, pdfs):
    self.data = database
    self.base = pdfs

  def __enter__(self):
    """
    Open the database
    """
    self.connect = sqlite3.connect(self.data)
    self.cursor = self.connect.cursor()
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    """
    Close the database
    """
    self.connect.commit()
    self.cursor.close()

  def get_document(self, doc_id):
    """
    Get document info using document id
    """
    self.cursor.execute(
        "SELECT uuid, citationKey, type, publication, year, title FROM \
            Documents WHERE id = ?", (doc_id, ))
    result = self.cursor.fetchone()
    if result:
      uuid, citation_key, pub_type, pub, year, title = result
      if citation_key is None:
        citation_key = ""
      if pub is None:
        pub = pub_type.upper()
      if year is None:
        year = "Year"
      if title is None:
        title = "Title"
      return (uuid, citation_key, pub, str(year), title)
    else:
      raise KeyError("Couldn't find document with id %s" % doc_id)

  def get_localUrl(self, file_hash):
    """
    Get the file directory 
    """
    self.cursor.execute(
        "SELECT localUrl FROM Files WHERE hash = ?", (file_hash, ))
    result = self.cursor.fetchone()
    if result:
      return result[0]
    else:
      raise KeyError("Couldn't find file with hash %s", file_hash)

  def folder_id(self, doc_id):
    """
    Get the folder id given document id
    """
    self.cursor.execute(
        "SELECT folderId FROM DocumentFolders WHERE documentId =?", (doc_id, ))
    result = self.cursor.fetchone()

    if result:
      return result[0]
    else:
      print "Couldn't find folderId with documentId %s" % doc_id
      return ''

  def get_folder_name(self, folder_id = None):
    """
    Get the folder name
    """
    if not folder_id:
      return u'Unsorted'

    self.cursor.execute(
        "SELECT name FROM Folders WHERE id = ?", (folder_id, ))
    result = self.cursor.fetchone()

    if result:
      return result[0]
    else:
      raise KeyError("Couldn't find name with folder id %s" % folder_id)

  def get_author_name(self, doc_id):
    """
    Get the last name of the first author
    """
    self.cursor.execute(
        "SELECT lastName FROM DocumentContributors WHERE documentId = ?", (doc_id, ))
    result = self.cursor.fetchone()
    if result:
      return result[0]
    else:
      print "No author found with documentId %s" % doc_id
      return 'Author'

  def get_new_dir(self, url, target_name, folder):
    """
    Obtain the desired directory
    """
    old_dir = urllib.unquote(url)
    old_dir = old_dir[7:]
    old_dir = os.path.abspath(old_dir)
    old_path = os.path.split(old_dir)[0]

    new_path = os.path.join(self.base, folder)
    new_path = os.path.abspath(new_path)
    new_dir = os.path.join(new_path, target_name) + '.pdf'

    old_path = old_path.encode('ascii', 'ignore')
    old_dir = old_dir.encode('ascii', 'ignore')
    new_path = new_path.encode('ascii', 'ignore')
    new_dir = new_dir.encode('ascii', 'ignore')

    new_url = u'file://' + urllib.quote(new_dir)

    return (old_path, old_dir, new_path, new_dir, new_url)

  def file_organizer(self):
    """
    Look at all files, change the file names, and
    move to the right location
    """

    self.cursor.execute("SELECT documentId, hash FROM DocumentFIles")

    for document_id, file_hash in self.cursor.fetchall():

      uuid, citation_key, pub, year, title = self.get_document(document_id)
      folder = self.get_folder_name(self.folder_id(document_id))
      author = self.get_author_name(document_id)
      url = self.get_localUrl(file_hash)

      #target_name = year + ' ' +  author + ' ' + pub + ' ' + title
      target_name = ' ' +  author + ' ' + year + ' ' + pub + ' ' + title
      target_name = re.split('[^a-zA-Z0-9]+', target_name)
      target_name = ' '.join(target_name).split()
      target_name = '_'.join(target_name)

      old_path, old_dir, new_path, new_dir, new_url = self.get_new_dir(url, target_name, folder)

      if os.path.isfile(old_dir):

        if new_url != url:
          self.cursor.execute("UPDATE Files SET localUrl=? WHERE hash=?", (new_url, file_hash))

        if new_dir !=old_dir:
          if not os.path.isdir(new_path):
            os.system('mkdir' + ' ' + repr(new_path))

          cmd = 'mv -iuv' + ' ' + repr(old_dir) + ' ' + repr(new_dir)
          os.system(cmd)

if __name__ == "__main__":
  database = sys.argv[1]
  pdfs = sys.argv[2]

  with MendeleyDB(database, pdfs) as mdl:
    mdl.file_organizer()



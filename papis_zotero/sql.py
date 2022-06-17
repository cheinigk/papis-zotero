#! /usr/bin/env python3
# papis-short-help: Convert zotero library to papis library using sqlite
# Copyright Felix Hummel 2017 GPLv3
# -*- coding: utf-8 -*-
import sqlite3
import yaml
import os
import shutil
import logging
import re

# zotero item types to be excluded.
# "attachment" are automatically excluded and will be treated as "files"
excludedTypes = ["note"]

# dictionary of zotero attachments mime types to be included
# mapped onto their respective extension to be used in papis
includedAttachments = {"application/vnd.ms-htmlhelp":  "chm",
                       "image/vnd.djvu": "djvu",
                       "application/msword":  "doc",
                       "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
                       "application/epub+zip": "epub",
                       "application/octet-stream":  "fb2",
                       "application/x-mobipocket-ebook": "mobi",
                       "application/pdf":  "pdf",
                       "text/rtf":  "rtf",
                       "application/zip":  "zip"}

# dictionary translating from zotero to papis type names
translatedTypes = {"journalArticle": "article"}

# dictionary translating from zotero to papis field names
translatedFields = {"DOI": "doi"}

# seperator between multiple tags
tagDelimiter = ","

# if no attachment is found, give info.yaml as content file
# set to None if no file should be given in that case
defaultFile = None


def getTuple(elements):
    """
    Concatenate given strings to SQL tuple of strings
    """
    elementsTuple = "("
    for element in elements:
        if elementsTuple != "(":
            elementsTuple += ","
        elementsTuple += "\"" + element + "\""
    elementsTuple += ")"
    return elementsTuple


def getFields(connection, itemId):
    itemFieldQuery = """
    SELECT
      fields.fieldName,
      itemDataValues.value
    FROM
      fields,
      itemData,
      itemDataValues
    WHERE
      itemData.itemID = {itemID} AND
      fields.fieldID = itemData.fieldID AND
      itemDataValues.valueID = itemData.valueID
    """
    fieldCursor = connection.cursor()
    fieldCursor.execute(itemFieldQuery.format(itemID=itemId))
    fields = {}
    for fieldRow in fieldCursor:
        fieldName = translatedFields.get(fieldRow[0], fieldRow[0])
        fieldValue = fieldRow[1]
        fields[fieldName] = fieldValue
    return fields


def getCreators(connection, itemId):
    itemCreatorQuery = """
    SELECT
      creatorTypes.creatorType,
      creators.firstName,
      creators.lastName
    FROM
      creatorTypes,
      creators,
      itemCreators
    WHERE
      itemCreators.itemID = {itemID} AND
      creatorTypes.creatorTypeID = itemCreators.creatorTypeID AND
      creators.creatorID = itemCreators.creatorID
    ORDER BY
      creatorTypes.creatorType,
      itemCreators.orderIndex
    """
    creatorCursor = connection.cursor()
    creatorCursor.execute(
        itemCreatorQuery.format(itemID=itemId)
    )
    creators = {}  # type: ignore

    for creatorRow in creatorCursor:
        creatorName = creatorRow[0]
        creatorNameList = creatorName + "_list"
        givenName = creatorRow[1]
        surname = creatorRow[2]

        currentCreators = creators.get(creatorName, "")
        if currentCreators != "":
            currentCreators += " and "
        currentCreators += "{surname}, {givenName}".format(
          givenName=givenName, surname=surname
        )
        creators[creatorName] = currentCreators

        currentCreatorsList = creators.get(creatorNameList, [])
        currentCreatorsList.append(
            {"given_name": givenName, "surname": surname}
        )
        creators[creatorNameList] = currentCreatorsList

    return creators


def getFiles(connection, itemId, itemKey, filesfolder):
    global inputPath
    mimeTypes = getTuple(includedAttachments.keys())
    itemAttachmentQuery = """
    SELECT
      items.key,
      itemAttachments.path,
      itemAttachments.contentType
    FROM
      itemAttachments,
      items
    WHERE
      itemAttachments.parentItemID = {itemID} AND
      itemAttachments.contentType IN {mimeTypes} AND
      items.itemID = itemAttachments.itemID
    """.format(itemID=itemId, mimeTypes=mimeTypes)
    attachmentCursor = connection.cursor()
    attachmentCursor.execute(itemAttachmentQuery)
    files = []
    for attachmentRow in attachmentCursor:
        key = attachmentRow[0]
        path = attachmentRow[1]

        mime = attachmentRow[2]
        # extension = includedAttachments[mime]
        importPath = ""

        try:
            # NOTE: a single file is assumed in the attachment's folder
            # to avoid using path, which may contain invalid characters
            if path.startswith("attachments:"):
                path = path.replace("attachments:", "")
                for ff in filesfolder:
                    importPath = os.path.join(ff, path)
                    if os.path.exists(importPath):
                        break
            elif path.startswith("storage:"):
                path = path.replace("storage:", "", 1)
                importPath = os.path.join(inputPath, "storage", key, path)
            localPath = os.path.join(
                outputPath, itemKey, os.path.basename(path)
            )
            shutil.copyfile(importPath, localPath)
            files.append(path)
        except shutil.SameFileError as err:
            print("SameFileError error: {0}".format(err))
            pass
        except OSError as err:
            print("OS error: {0}".format(err))
            pass
        except BaseException as err:
            print("error: {0}".format(err))
            print(
              "failed to export attachment {key}: {path} ({mime})".format(
                key=key, path=importPath, mime=mime
              )
            )
            pass

    if files == [] and defaultFile:
        files.append(defaultFile)
    return {"files": files}


def getTags(connection, itemId):
    itemTagQuery = """
    SELECT
      tags.name
    FROM
      tags,
      itemTags
    WHERE
      itemTags.itemID = {itemID} AND
      tags.tagID = itemTags.tagID
    """.format(itemID=itemId)
    tagCursor = connection.cursor()
    tagCursor.execute(itemTagQuery)
    tags = ""
    for tagRow in tagCursor:
        if tags != "":
            tags += tagDelimiter + " "
        tags += "{tag}".format(tag=tagRow[0])

    return {"tags": tags}


def getCollections(connection, itemId):
    itemCollectionQuery = """
      SELECT
        collections.collectionName
      FROM
        collections,
        collectionItems
      WHERE
        collectionItems.itemID = {itemID} AND
        collections.collectionID = collectionItems.collectionID
    """.format(itemID=itemId)
    collectionCursor = connection.cursor()
    collectionCursor.execute(itemCollectionQuery)
    collections = []
    for collectionRow in collectionCursor:
        collections.append(collectionRow[0])

    return {"project": collections}



###############################################################################

def add_from_sql(input_path, output_path, filesfolder):
    """

    :param input_path: path to zotero SQLite database "zoter.sqlite" and
        "storage" to be imported
    :param output_path: path where all items will be exported to created if not
        existing
    """
    global inputPath
    global outputPath

    logger = logging.getLogger('papis_zotero:importer:sql')
    inputPath = input_path
    outputPath = output_path

    connection = sqlite3.connect(os.path.join(inputPath, "zotero.sqlite"))
    cursor = connection.cursor()

    excludedTypes.append("attachment")
    excludedTypeTuple = getTuple(excludedTypes)
    itemsCountQuery = """
      SELECT
        COUNT(item.itemID)
      FROM
        items item,
        itemTypes itemType
      WHERE
        itemType.itemTypeID = item.itemTypeID AND
        itemType.typeName NOT IN {excludedTypeTuple}
      ORDER BY
        item.itemID
    """.format(excludedTypeTuple=excludedTypeTuple)
    cursor.execute(itemsCountQuery)
    itemsCount = 0
    for row in cursor:
        itemsCount = row[0]

    itemsQuery = """
      SELECT
        item.itemID,
        itemType.typeName,
        key,
        dateAdded,
        dateModified,
        clientDateModified
      FROM
        items item,
        itemTypes itemType
      WHERE
        itemType.itemTypeID = item.itemTypeID AND
        itemType.typeName NOT IN {excludedTypeTuple}
      ORDER BY
        item.itemID
    """.format(excludedTypeTuple=excludedTypeTuple)

    cursor.execute(itemsQuery)
    currentItem = 0
    for row in cursor:
        currentItem += 1
        itemId = row[0]
        itemType = translatedTypes.get(row[1], row[1])
        itemKey = row[2]
        dateAdded = row[3]
        dateModified = row[4]
        clientDateModified = row[5]
        logger.info(
            "exporting item {currentItem}/{itemsCount}: {key}".format(
                currentItem=currentItem, itemsCount=itemsCount, key=itemKey
            )
        )

        path = os.path.join(outputPath, itemKey)
        if not os.path.exists(path):
            os.makedirs(path)

        # Get mendeley keys
        fields = getFields(connection, itemId)
        extra = fields.get("extra", None)
        ref = itemKey
        if extra:
            # try to convert
            matches = re.search(r'.*Citation Key: (\w+)', extra)
            if matches:
                ref = matches.group(1)
        logger.info("exporting under ref %s" % ref)
        item = { "ref": ref
               , "type": itemType
               , "created": dateAdded
               , "modified": dateModified
               , "modified.client": clientDateModified
               }
        item.update(fields)
        item.update(getCreators(connection, itemId))
        item.update(getTags(connection, itemId))
        item.update(getCollections(connection, itemId))
        item.update(getFiles(connection, itemId, itemKey, filesfolder))

        item.update({"ref": ref})

        with open(os.path.join(path, "info.yaml"), "w+") as itemFile:
            yaml.dump(item, itemFile, default_flow_style=False, allow_unicode=True)

    logger.info("done")

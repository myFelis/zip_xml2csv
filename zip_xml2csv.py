import asyncio
import zipfile
import logging
from datetime import datetime
from os import path, walk, remove
from utils import ValidationError, COUNT_XML_IN_ZIP, ROOT_DIR, STORE_DIR, \
    RESULT_DIR, XML_EXTENSION, ZIP_COUNT, random_xml, write_csv, write_file_ids


logging.basicConfig(filename="xml2csv.log", level=logging.INFO)


async def verify_file(loaded_file, file_name):
    # logging to know wrong xmls for checking
    if not hasattr(loaded_file, 'name') or not loaded_file.name:
        message = f'Unavailable file object {file_name} in archive.'
        logging.warning(message)
        raise ValidationError(message, code='file_object_error')
    file_name, file_extension = path.splitext(loaded_file.name)
    if not file_extension or not file_extension == '.' + XML_EXTENSION:
        message = f'Wrong object file {file_name} type, {XML_EXTENSION} expected.'
        logging.warning(message)
        raise ValidationError(message, code='file_type_error')
    return file_name


async def zip2csv(archived_files, root_dir=STORE_DIR, result=RESULT_DIR):
    for archive_name in archived_files:
        archived = path.join(root_dir, archive_name)
        try:
            with zipfile.ZipFile(archived, mode='r') as container:
                file_names = container.namelist()
                if len(file_names) != COUNT_XML_IN_ZIP:
                    message = f'Wrong files count in {archive_name}, {COUNT_XML_IN_ZIP} expected.'
                    logging.error(message)
                    # support for wrong files count in the zip is not expected, raise
                    raise ValidationError(message, code='invalid_files_count')
                await write_file_ids(file_names, result)
                for file_name in file_names:
                    loaded_file = open(container.extract(file_name, path=STORE_DIR), mode='r+')
                    try:
                        await verify_file(loaded_file, file_name)
                        await write_csv(loaded_file, result)
                    except ValidationError:
                        # can parse for other available xmls in the zip, so continue
                        continue
                    finally:
                        remove(path.join(STORE_DIR, file_name))
        except zipfile.BadZipFile:
            logging.warning('BadZipFile in store directory')
            continue
    return


async def xml2zip(xml_names=None, root_dir=ROOT_DIR, store_dir=STORE_DIR):
    zipped = (f'0{n}zip.zip' for n in range(1, ZIP_COUNT))
    for arcname in zipped:
        names = xml_names or await random_xml(root_dir)
        with zipfile.ZipFile(path.join(store_dir, arcname), 'w') as zipped:
            for file_name in names:
                zipped.write(file_name[0], file_name[1])
                remove(file_name[0])
    return


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        logging.info(f"xml2zip started at {datetime.now()}")
        loop.run_until_complete(xml2zip())
        logging.info(f"zip2csv started at {datetime.now()}")
        for root, dirs, files in walk(STORE_DIR):
            loop.run_until_complete(zip2csv(files, root))
    except Exception as e:
        logging.error(f"error {e}")
    finally:
        for root, dirs, files in walk(path.join(ROOT_DIR, 'tmp')):
            for name in files:
                remove(path.join(root, name))
        logging.info(f"xml2csv finished at {datetime.now()}")

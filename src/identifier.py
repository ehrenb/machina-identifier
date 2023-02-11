import base64
from datetime import datetime
import json
import hashlib
import os
from pprint import pformat

import magic

from machina.core.worker import Worker
from machina.core.models import Artifact, Base
from machina.core.models.utils import resolve_db_node_cls

class Identifier(Worker):
    """Identifier is the entrypoint to the system
    therefore, it does require any types, and can only be
    invoked directly by publishing to the Identifier queue
    """
    next_queues = []
    types = []

    def __init__(self, *args, **kwargs):
        super(Identifier, self).__init__(*args, **kwargs)

    def callback(self, data, properties):
        data = json.loads(data)
        binary_data = data['data']

        self.logger.debug(pformat(data))

        def _hash_data(data):
            result = {}
            for a in self.config['worker']['hash_algorithms']:
                h = hashlib.new(a, binary_data.encode()).hexdigest()
                result[a] = h
            return result

        def _resolve_supported_type(fpath):
            """attempt to resolve the type of a binary file
            against the high-level types loaded into the configuration
            Return a justification (e.g. the type, and the justification (e.g. mime, detailed)
            Prioritize detailed_types over mime
            """

            detailed_type = magic.from_file(fpath)
            for d, t in self.config['types']['detailed_types'].items():
                if detailed_type.lower().startswith(d.lower()):
                    return dict(type=t,
                                reason="detailed_types",
                                value=d)

            mime = magic.from_file(fpath, mime=True)
            for m, t in self.config['types']['mimes'].items():
                if mime == m:
                    return dict(type=t,
                                reason="mimes",
                                value=m)
            return None


        data_decoded = base64.b64decode(binary_data)

        # hash data_decoded
        hashes = _hash_data(data_decoded)
        
        # dir is time stamp
        ts = datetime.now()
        ts_fs = ts.strftime("%Y%m%d%H%M%S%f")

        binary_dir = os.path.join(self.config['paths']['binaries'], ts_fs)
        if not os.path.isdir(binary_dir):
            os.makedirs(binary_dir)

        # fname is md5
        binary_fpath = os.path.join(binary_dir, hashes['md5'])
        with open(binary_fpath, 'wb') as f:
            f.write(data_decoded)

        # file size
        size = os.path.getsize(binary_fpath)

        # Flag to see if a manually-set type is supported
        supported_type = False

        # If type was specified, take that as ground truth
        if 'type' in data.keys():
            resolved_type = data['type']
            resolved = dict(
                type=resolved_type,
                reason="provided",
                value=None
            )

            if resolved_type in self.config['types']['available_types']:
                supported_type = True
            else:
                self.logger.error(f"provided type: {resolved_type} is not in system available types: {pformat(self.config['types']['available_types'])}")
                return 

        # else, attempt to derive it using magic
        else:
            resolved = _resolve_supported_type(binary_fpath)
            if resolved:
                resolved_type = resolved['type']
            else:
                # If not resolved to a supported Machine type, it will become an Artifact node
                self.logger.warn(f"{binary_fpath} type couldn't be resolved to supported type, is it supported? defaulted to  artifact")
                resolved_type = 'artifact' 
                resolved = resolved = dict(
                    type=resolved_type,
                    reason="defaulted",
                    value=None
                )

                
        self.logger.info(f"resolved to: {resolved}")

        body = {
            'ts': ts_fs,
            'hashes': hashes,
            'type': resolved_type
        }

        # Dynamic class resolution for Machina type -> OrientDB Node class
        # These are coupled tightly, a Node class' element_type attribute is named the same as a type
        # and the search ignores case
        c = resolve_db_node_cls(resolved_type)
        node = c(
            md5=body['hashes']['md5'],
            sha256=body['hashes']['sha256'],
            size=size,
            ts=ts,
            type=resolved_type).save()

        body['uid'] = node.uid #node.id
        
        # If specified, link to another run's artifact
        # This is useful during the retyping process
        # Or to assert a link manually
        origin_node = None
        if 'origin' in data.keys():
            # Retrieve the originating Node cls from the database
            # origin_node = self.graph.get_vertex(data['origin']['id'])
            # resolve the originating node's OGMY class type
            origin_node_cls = resolve_db_node_cls(data['origin']['type'])
            origin_node = origin_node_cls.nodes.get_or_none(uid=data['origin']['uid'])

        # If the resolved originating hash matches the given hash
        # this is a retype of a previous Node.
        # If no match, it's an extraction
        if origin_node:
            if origin_node.md5 == body['hashes']['md5']:
                # create relationship (retype) btwn a and origin_a
                self.logger.info("Establishing retype link")
                retyped_rel = origin_node.retyped.connect(node).save()
            else:
                self.logger.info("Establishing extraction link")
                extract_rel = origin_node.extracts.connect(node).save()

        self.publish(
            json.dumps(body),
            [resolved_type]
        )

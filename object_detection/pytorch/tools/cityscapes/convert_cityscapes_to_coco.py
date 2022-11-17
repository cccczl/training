# Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#!/usr/bin/env python

# Copyright (c) 2017-present, Facebook, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##############################################################################

# This file is copy from https://github.com/facebookresearch/Detectron/tree/master/tools

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import h5py
import json
import os
import scipy.misc
import sys

import cityscapesscripts.evaluation.instances2dict_with_polygons as cs

import detectron.utils.segms as segms_util
import detectron.utils.boxes as bboxs_util


def parse_args():
    parser = argparse.ArgumentParser(description='Convert dataset')
    parser.add_argument(
        '--dataset', help="cocostuff, cityscapes", default=None, type=str)
    parser.add_argument(
        '--outdir', help="output dir for json files", default=None, type=str)
    parser.add_argument(
        '--datadir', help="data dir for annotations to be converted",
        default=None, type=str)
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    return parser.parse_args()


def convert_coco_stuff_mat(data_dir, out_dir):
    """Convert to png and save json with path. This currently only contains
    the segmentation labels for objects+stuff in cocostuff - if we need to
    combine with other labels from original COCO that will be a TODO."""
    sets = ['train', 'val']
    categories = []
    json_name = 'coco_stuff_%s.json'
    ann_dict = {}
    for data_set in sets:
        file_list = os.path.join(data_dir, '%s.txt')
        images = []
        with open(file_list % data_set) as f:
            for img_id, img_name in enumerate(f):
                img_name = img_name.replace('coco', 'COCO').strip('\n')
                mat_file = os.path.join(data_dir, f'annotations/{img_name}.mat')
                data = h5py.File(mat_file, 'r')
                labelMap = data.get('S')
                if not categories:
                    labelNames = data.get('names')
                    categories.extend(
                        {
                            "id": idx,
                            "name": ''.join(chr(i) for i in data[n[0]]),
                        }
                        for idx, n in enumerate(labelNames)
                    )

                    ann_dict['categories'] = categories
                scipy.misc.imsave(os.path.join(data_dir, f'{img_name}.png'), labelMap)
                image = {
                    'width': labelMap.shape[0],
                    'height': labelMap.shape[1],
                    'file_name': img_name,
                    'seg_file_name': img_name,
                    'id': img_id,
                }

                images.append(image)
        ann_dict['images'] = images
        print(f"Num images: {len(images)}")
        with open(os.path.join(out_dir, json_name % data_set), 'wb') as outfile:
            outfile.write(json.dumps(ann_dict))


# for Cityscapes
def getLabelID(self, instID):
    return instID if (instID < 1000) else int(instID / 1000)


def convert_cityscapes_instance_only(
        data_dir, out_dir):
    """Convert from cityscapes format to COCO instance seg format - polygons"""
    sets = [
        'gtFine_val',
        'gtFine_train',
        'gtFine_test',

        # 'gtCoarse_train',
        # 'gtCoarse_val',
        # 'gtCoarse_train_extra'
    ]
    ann_dirs = [
        'gtFine_trainvaltest/gtFine/val',
        'gtFine_trainvaltest/gtFine/train',
        'gtFine_trainvaltest/gtFine/test',

        # 'gtCoarse/train',
        # 'gtCoarse/train_extra',
        # 'gtCoarse/val'
    ]
    json_name = 'instancesonly_filtered_%s.json'
    ends_in = '%s_polygons.json'
    img_id = 0
    ann_id = 0
    cat_id = 1
    category_dict = {}

    category_instancesonly = [
        'person',
        'rider',
        'car',
        'truck',
        'bus',
        'train',
        'motorcycle',
        'bicycle',
    ]

    for data_set, ann_dir in zip(sets, ann_dirs):
        print(f'Starting {data_set}')
        images = []
        annotations = []
        ann_dir = os.path.join(data_dir, ann_dir)
        for root, _, files in os.walk(ann_dir):
            for filename in files:
                if filename.endswith(ends_in % data_set.split('_')[0]):
                    if len(images) % 50 == 0:
                        print(f"Processed {len(images)} images, {len(annotations)} annotations")
                    json_ann = json.load(open(os.path.join(root, filename)))
                    image = {'id': img_id}
                    img_id += 1

                    image['width'] = json_ann['imgWidth']
                    image['height'] = json_ann['imgHeight']
                    image['file_name'] = filename[:-len(
                        ends_in % data_set.split('_')[0])] + 'leftImg8bit.png'
                    image['seg_file_name'] = (
                        filename[: -len(ends_in % data_set.split('_')[0])]
                        + f"{data_set.split('_')[0]}_instanceIds.png"
                    )

                    images.append(image)

                    fullname = os.path.join(root, image['seg_file_name'])
                    objects = cs.instances2dict_with_polygons(
                        [fullname], verbose=False)[fullname]

                    for object_cls in objects:
                        if object_cls not in category_instancesonly:
                            continue  # skip non-instance categories

                        for obj in objects[object_cls]:
                            if obj['contours'] == []:
                                print('Warning: empty contours.')
                                continue  # skip non-instance categories

                            len_p = [len(p) for p in obj['contours']]
                            if min(len_p) <= 4:
                                print('Warning: invalid contours.')
                                continue  # skip non-instance categories

                            ann = {'id': ann_id}
                            ann_id += 1
                            ann['image_id'] = image['id']
                            ann['segmentation'] = obj['contours']

                            if object_cls not in category_dict:
                                category_dict[object_cls] = cat_id
                                cat_id += 1
                            ann['category_id'] = category_dict[object_cls]
                            ann['iscrowd'] = 0
                            ann['area'] = obj['pixelCount']
                            ann['bbox'] = bboxs_util.xyxy_to_xywh(
                                segms_util.polys_to_boxes(
                                    [ann['segmentation']])).tolist()[0]

                            annotations.append(ann)

        ann_dict = {'images': images}
        categories = [{"id": category_dict[name], "name": name} for name in
                      category_dict]
        ann_dict['categories'] = categories
        ann_dict['annotations'] = annotations
        print(f"Num categories: {len(categories)}")
        print(f"Num images: {len(images)}")
        print(f"Num annotations: {len(annotations)}")
        with open(os.path.join(out_dir, json_name % data_set), 'w') as outfile:
            outfile.write(json.dumps(ann_dict))


if __name__ == '__main__':
    args = parse_args()
    if args.dataset == "cityscapes_instance_only":
        convert_cityscapes_instance_only(args.datadir, args.outdir)
    elif args.dataset == "cocostuff":
        convert_coco_stuff_mat(args.datadir, args.outdir)
    else:
        print(f"Dataset not supported: {args.dataset}")

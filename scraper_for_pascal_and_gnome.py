"""
Scraping script for PASCAL VOC and Visual Genome datasets.
Supports keyword-based filtering of images and annotations.
uv run scraper_for_pascal_and_gnome.py --dataset pascal_voc --keyword dog --max-images 50
"""

import os
import json
import xml.etree.ElementTree as ET
import requests
import shutil
import logging
from enum import Enum
from typing import List, Dict, Optional, Set, Tuple
from pathlib import Path
from urllib.parse import urljoin
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ─── Configuration ───────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class DatasetType(Enum):
    PASCAL_VOC = "pascal_voc"
    VISUAL_GENOME = "visual_genome"


@dataclass
class ScrapingConfig:
    """Configuration for the scraping process."""
    dataset_type: DatasetType
    keywords: List[str]
    output_dir: str = "./scraped_data"
    max_images: int = 1000
    num_workers: int = 4
    download_images: bool = True
    save_annotations: bool = True
    min_object_size: int = 0  # minimum bounding box area
    case_sensitive: bool = False
    # PASCAL VOC specific
    voc_year: str = "2012"  # "2007" or "2012"
    voc_root: str = ""  # path to local VOC dataset
    # Visual Genome specific
    vg_api_base: str = "https://homes.cs.washington.edu/~ranjay/visualgenome/api/v0"
    vg_data_dir: str = ""  # path to local VG data files
    vg_image_base_url: str = "https://cs.stanford.edu/people/rak248/VG_100K"


# ─── Data Models ─────────────────────────────────────────────────────────────

@dataclass
class BoundingBox:
    x_min: float
    y_min: float
    x_max: float
    y_max: float

    @property
    def width(self) -> float:
        return self.x_max - self.x_min

    @property
    def height(self) -> float:
        return self.y_max - self.y_min

    @property
    def area(self) -> float:
        return self.width * self.height

    def to_dict(self) -> Dict:
        return {
            "x_min": self.x_min,
            "y_min": self.y_min,
            "x_max": self.x_max,
            "y_max": self.y_max,
            "width": self.width,
            "height": self.height,
        }


@dataclass
class ObjectAnnotation:
    label: str
    bbox: BoundingBox
    attributes: Dict = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "label": self.label,
            "bbox": self.bbox.to_dict(),
            "attributes": self.attributes,
        }


@dataclass
class ImageRecord:
    image_id: str
    filename: str
    url: str
    width: int
    height: int
    objects: List[ObjectAnnotation] = field(default_factory=list)
    relationships: List[Dict] = field(default_factory=list)
    source: str = ""

    def to_dict(self) -> Dict:
        return {
            "image_id": self.image_id,
            "filename": self.filename,
            "url": self.url,
            "width": self.width,
            "height": self.height,
            "objects": [obj.to_dict() for obj in self.objects],
            "relationships": self.relationships,
            "source": self.source,
        }


# ─── Base Scraper ────────────────────────────────────────────────────────────

class BaseScraper:
    """Base class for dataset scrapers."""

    def __init__(self, config: ScrapingConfig):
        self.config = config
        self.keywords = self._normalize_keywords(config.keywords)
        self.results: List[ImageRecord] = []
        self._setup_output_dirs()

    def _normalize_keywords(self, keywords: List[str]) -> Set[str]:
        if self.config.case_sensitive:
            return set(keywords)
        return {kw.lower() for kw in keywords}

    def _setup_output_dirs(self):
        self.output_dir = Path(self.config.output_dir) / self.config.dataset_type.value
        self.images_dir = self.output_dir / "images"
        self.annotations_dir = self.output_dir / "annotations"
        self.images_dir.mkdir(parents=True, exist_ok=True)
        self.annotations_dir.mkdir(parents=True, exist_ok=True)

    def _matches_keyword(self, label: str) -> bool:
        """Check if a label matches any of the keywords."""
        compare_label = label if self.config.case_sensitive else label.lower()
        for keyword in self.keywords:
            # Exact match or substring match
            if keyword in compare_label or compare_label in keyword:
                return True
        return False

    def _download_image(self, url: str, save_path: Path) -> bool:
        """Download an image from URL."""
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            with open(save_path, "wb") as f:
                shutil.copyfileobj(response.raw, f)
            return True
        except Exception as e:
            logger.warning(f"Failed to download {url}: {e}")
            return False

    def scrape(self) -> List[ImageRecord]:
        raise NotImplementedError

    def save_results(self):
        """Save scraped results to JSON."""
        output_file = self.output_dir / "results.json"
        data = {
            "dataset": self.config.dataset_type.value,
            "keywords": list(self.keywords),
            "total_images": len(self.results),
            "images": [record.to_dict() for record in self.results],
        }
        with open(output_file, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Results saved to {output_file}")

    def save_per_image_annotation(self, record: ImageRecord):
        """Save individual annotation file per image."""
        ann_file = self.annotations_dir / f"{record.image_id}.json"
        with open(ann_file, "w") as f:
            json.dump(record.to_dict(), f, indent=2)


# ─── PASCAL VOC Scraper ─────────────────────────────────────────────────────

class PascalVOCScraper(BaseScraper):
    """
    Scraper for PASCAL VOC dataset.
    Can work with:
    1. Local VOC dataset directory
    2. Download from mirror URLs
    """

    # VOC download URLs
    VOC_URLS = {
        "2007": {
            "trainval": "http://host.robots.ox.ac.uk/pascal/VOC/voc2007/VOCtrainval_06-Nov-2007.tar",
            "test": "http://host.robots.ox.ac.uk/pascal/VOC/voc2007/VOCtest_06-Nov-2007.tar",
        },
        "2012": {
            "trainval": "http://host.robots.ox.ac.uk/pascal/VOC/voc2012/VOCtrainval_11-May-2012.tar",
        },
    }

    # All 20 PASCAL VOC classes
    VOC_CLASSES = [
        "aeroplane", "bicycle", "bird", "boat", "bottle",
        "bus", "car", "cat", "chair", "cow",
        "diningtable", "dog", "horse", "motorbike", "person",
        "pottedplant", "sheep", "sofa", "train", "tvmonitor",
    ]

    def __init__(self, config: ScrapingConfig):
        super().__init__(config)
        self.voc_root = Path(config.voc_root) if config.voc_root else None
        self._matched_classes = self._get_matched_classes()

        if self._matched_classes:
            logger.info(f"Matched VOC classes: {self._matched_classes}")
        else:
            logger.warning(
                f"No VOC classes match keywords: {self.keywords}. "
                f"Available classes: {self.VOC_CLASSES}"
            )

    def _get_matched_classes(self) -> Set[str]:
        """Find VOC classes that match the given keywords."""
        matched = set()
        for cls in self.VOC_CLASSES:
            if self._matches_keyword(cls):
                matched.add(cls)
        return matched

    def _find_voc_root(self) -> Optional[Path]:
        """Locate VOC dataset root directory."""
        if self.voc_root and self.voc_root.exists():
            # Try standard VOC directory structure
            voc_dir = self.voc_root / f"VOC{self.config.voc_year}"
            if voc_dir.exists():
                return voc_dir
            # Maybe the root IS the VOC directory
            if (self.voc_root / "Annotations").exists():
                return self.voc_root
        return None

    def _parse_voc_annotation(self, xml_path: Path) -> Optional[ImageRecord]:
        """Parse a PASCAL VOC XML annotation file."""
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()

            filename = root.find("filename").text
            size = root.find("size")
            width = int(size.find("width").text)
            height = int(size.find("height").text)

            image_id = xml_path.stem

            objects = []
            has_matching_object = False

            for obj in root.findall("object"):
                name = obj.find("name").text
                difficult = int(obj.find("difficult").text) if obj.find("difficult") is not None else 0

                bbox_elem = obj.find("bndbox")
                bbox = BoundingBox(
                    x_min=float(bbox_elem.find("xmin").text),
                    y_min=float(bbox_elem.find("ymin").text),
                    x_max=float(bbox_elem.find("xmax").text),
                    y_max=float(bbox_elem.find("ymax").text),
                )

                if bbox.area < self.config.min_object_size:
                    continue

                annotation = ObjectAnnotation(
                    label=name,
                    bbox=bbox,
                    attributes={
                        "difficult": difficult,
                        "truncated": int(obj.find("truncated").text) if obj.find("truncated") is not None else 0,
                        "pose": obj.find("pose").text if obj.find("pose") is not None else "Unspecified",
                    }
                )
                objects.append(annotation)

                if self._matches_keyword(name):
                    has_matching_object = True

            if not has_matching_object:
                return None

            # Filter to only matching objects if desired (or keep all objects in matched images)
            matching_objects = [obj for obj in objects if self._matches_keyword(obj.label)]

            return ImageRecord(
                image_id=image_id,
                filename=filename,
                url="",  # local file
                width=width,
                height=height,
                objects=matching_objects,
                source=f"pascal_voc_{self.config.voc_year}",
            )

        except Exception as e:
            logger.error(f"Error parsing {xml_path}: {e}")
            return None

    def _scrape_from_local(self, voc_dir: Path) -> List[ImageRecord]:
        """Scrape from local PASCAL VOC dataset."""
        annotations_dir = voc_dir / "Annotations"
        images_dir = voc_dir / "JPEGImages"

        if not annotations_dir.exists():
            logger.error(f"Annotations directory not found: {annotations_dir}")
            return []

        xml_files = sorted(annotations_dir.glob("*.xml"))
        logger.info(f"Found {len(xml_files)} annotation files")

        results = []
        for xml_path in tqdm(xml_files, desc="Parsing VOC annotations"):
            if len(results) >= self.config.max_images:
                break

            record = self._parse_voc_annotation(xml_path)
            if record is None:
                continue

            # Copy image if download_images is True
            if self.config.download_images and images_dir.exists():
                src_image = images_dir / record.filename
                if src_image.exists():
                    dst_image = self.images_dir / record.filename
                    shutil.copy2(src_image, dst_image)
                    record.url = str(dst_image)

            if self.config.save_annotations:
                self.save_per_image_annotation(record)

            results.append(record)

        return results

    def _scrape_from_imageset(self, voc_dir: Path) -> List[ImageRecord]:
        """
        Use ImageSets to find images containing specific classes.
        More efficient than scanning all annotations.
        """
        imagesets_dir = voc_dir / "ImageSets" / "Main"
        if not imagesets_dir.exists():
            return self._scrape_from_local(voc_dir)

        target_image_ids = set()

        for cls in self._matched_classes:
            for split in ["trainval", "train", "val"]:
                set_file = imagesets_dir / f"{cls}_{split}.txt"
                if set_file.exists():
                    with open(set_file, "r") as f:
                        for line in f:
                            parts = line.strip().split()
                            if len(parts) == 2:
                                img_id, flag = parts
                                if int(flag) == 1:  # 1 = present, -1 = absent
                                    target_image_ids.add(img_id)
                    break  # Use first available split

        logger.info(f"Found {len(target_image_ids)} images with matching classes via ImageSets")

        annotations_dir = voc_dir / "Annotations"
        images_dir = voc_dir / "JPEGImages"
        results = []

        for img_id in tqdm(sorted(target_image_ids), desc="Processing matched images"):
            if len(results) >= self.config.max_images:
                break

            xml_path = annotations_dir / f"{img_id}.xml"
            if not xml_path.exists():
                continue

            record = self._parse_voc_annotation(xml_path)
            if record is None:
                continue

            if self.config.download_images and images_dir.exists():
                src_image = images_dir / record.filename
                if src_image.exists():
                    dst_image = self.images_dir / record.filename
                    shutil.copy2(src_image, dst_image)
                    record.url = str(dst_image)

            if self.config.save_annotations:
                self.save_per_image_annotation(record)

            results.append(record)

        return results

    def _download_voc_dataset(self) -> Optional[Path]:
        """Download PASCAL VOC dataset."""
        import tarfile

        year = self.config.voc_year
        if year not in self.VOC_URLS:
            logger.error(f"Unsupported VOC year: {year}")
            return None

        download_dir = Path(self.config.output_dir) / "voc_download"
        download_dir.mkdir(parents=True, exist_ok=True)

        url = self.VOC_URLS[year]["trainval"]
        tar_path = download_dir / f"VOC{year}.tar"

        if not tar_path.exists():
            logger.info(f"Downloading PASCAL VOC {year} from {url}...")
            try:
                response = requests.get(url, stream=True, timeout=300)
                response.raise_for_status()
                total_size = int(response.headers.get("content-length", 0))

                with open(tar_path, "wb") as f:
                    with tqdm(total=total_size, unit="B", unit_scale=True, desc="Downloading") as pbar:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                            pbar.update(len(chunk))
            except Exception as e:
                logger.error(f"Failed to download VOC dataset: {e}")
                return None

        # Extract
        extract_dir = download_dir / "extracted"
        if not extract_dir.exists():
            logger.info("Extracting dataset...")
            with tarfile.open(tar_path, "r") as tar:
                tar.extractall(extract_dir)

        voc_dir = extract_dir / "VOCdevkit" / f"VOC{year}"
        if voc_dir.exists():
            return voc_dir

        return None

    def scrape(self) -> List[ImageRecord]:
        """Main scraping entry point for PASCAL VOC."""
        if not self._matched_classes:
            logger.warning("No matching classes found. Returning empty results.")
            return []

        # Try local first
        voc_dir = self._find_voc_root()

        if voc_dir is None:
            logger.info("Local VOC dataset not found. Attempting download...")
            voc_dir = self._download_voc_dataset()

        if voc_dir is None:
            logger.error("Could not find or download PASCAL VOC dataset.")
            return []

        logger.info(f"Using VOC directory: {voc_dir}")

        # Use ImageSets for efficient lookup
        self.results = self._scrape_from_imageset(voc_dir)

        logger.info(f"Scraped {len(self.results)} images from PASCAL VOC")
        self.save_results()
        return self.results


# ─── Visual Genome Scraper ───────────────────────────────────────────────────

class VisualGenomeScraper(BaseScraper):
    """
    Scraper for Visual Genome dataset.
    Can work with:
    1. Local VG data files (objects.json, image_data.json, etc.)
    2. VG API
    """

    VG_DATA_URLS = {
        "image_data": "https://homes.cs.washington.edu/~ranjay/visualgenome/data/dataset/image_data.json.zip",
        "objects": "https://homes.cs.washington.edu/~ranjay/visualgenome/data/dataset/objects.json.zip",
        "relationships": "https://homes.cs.washington.edu/~ranjay/visualgenome/data/dataset/relationships.json.zip",
        "attributes": "https://homes.cs.washington.edu/~ranjay/visualgenome/data/dataset/attributes.json.zip",
        "region_descriptions": "https://homes.cs.washington.edu/~ranjay/visualgenome/data/dataset/region_descriptions.json.zip",
    }

    def __init__(self, config: ScrapingConfig):
        super().__init__(config)
        self.vg_data_dir = Path(config.vg_data_dir) if config.vg_data_dir else None
        self._image_data_cache: Dict = {}
        self._objects_cache: Optional[List] = None

    def _load_json_file(self, filepath: Path) -> Optional[any]:
        """Load a JSON file with progress indication."""
        logger.info(f"Loading {filepath}...")
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
            logger.info(f"Loaded {len(data) if isinstance(data, list) else 'N/A'} entries from {filepath.name}")
            return data
        except Exception as e:
            logger.error(f"Failed to load {filepath}: {e}")
            return None

    def _download_vg_data(self, data_key: str) -> Optional[Path]:
        """Download Visual Genome data file."""
        import zipfile

        if data_key not in self.VG_DATA_URLS:
            return None

        download_dir = Path(self.config.output_dir) / "vg_download"
        download_dir.mkdir(parents=True, exist_ok=True)

        url = self.VG_DATA_URLS[data_key]
        zip_path = download_dir / f"{data_key}.json.zip"
        json_path = download_dir / f"{data_key}.json"

        if json_path.exists():
            return json_path

        if not zip_path.exists():
            logger.info(f"Downloading {data_key} from {url}...")
            try:
                response = requests.get(url, stream=True, timeout=600)
                response.raise_for_status()
                total_size = int(response.headers.get("content-length", 0))
                with open(zip_path, "wb") as f:
                    with tqdm(total=total_size, unit="B", unit_scale=True, desc=f"Downloading {data_key}") as pbar:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                            pbar.update(len(chunk))
            except Exception as e:
                logger.error(f"Failed to download {data_key}: {e}")
                return None

        # Extract
        try:
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(download_dir)
            # Find the extracted JSON file
            for f in download_dir.glob("*.json"):
                if data_key in f.name:
                    return f
        except Exception as e:
            logger.error(f"Failed to extract {zip_path}: {e}")

        return json_path if json_path.exists() else None

    def _get_data_path(self, data_key: str) -> Optional[Path]:
        """Get path to a VG data file, downloading if necessary."""
        # Check local directory first
        if self.vg_data_dir:
            local_path = self.vg_data_dir / f"{data_key}.json"
            if local_path.exists():
                return local_path

        # Try download directory
        download_dir = Path(self.config.output_dir) / "vg_download"
        cached_path = download_dir / f"{data_key}.json"
        if cached_path.exists():
            return cached_path

        # Download
        return self._download_vg_data(data_key)

    def _load_image_data(self) -> Dict[int, Dict]:
        """Load image metadata indexed by image_id."""
        if self._image_data_cache:
            return self._image_data_cache

        data_path = self._get_data_path("image_data")
        if data_path is None:
            return {}

        raw_data = self._load_json_file(data_path)
        if raw_data is None:
            return {}

        self._image_data_cache = {}
        for item in raw_data:
            img_id = item.get("image_id") or item.get("id")
            if img_id:
                self._image_data_cache[img_id] = item

        return self._image_data_cache

    def _load_objects(self) -> Optional[List]:
        """Load objects data."""
        if self._objects_cache is not None:
            return self._objects_cache

        data_path = self._get_data_path("objects")
        if data_path is None:
            return None

        self._objects_cache = self._load_json_file(data_path)
        return self._objects_cache

    def _scrape_from_local_objects(self) -> List[ImageRecord]:
        """Scrape from local objects.json file."""
        objects_data = self._load_objects()
        if objects_data is None:
            return []

        image_data = self._load_image_data()
        results = []

        for image_objects in tqdm(objects_data, desc="Scanning Visual Genome objects"):
            if len(results) >= self.config.max_images:
                break

            image_id = image_objects.get("image_id")
            objects_list = image_objects.get("objects", [])

            # Check if any object matches our keywords
            matching_objects = []
            for obj in objects_list:
                names = obj.get("names", [])
                if isinstance(names, str):
                    names = [names]

                # Also check synsets
                synsets = obj.get("synsets", [])

                all_labels = names + synsets
                matched = False
                matched_label = ""

                for label in all_labels:
                    if self._matches_keyword(label):
                        matched = True
                        matched_label = label
                        break

                if matched:
                    # Handle different bbox formats in VG
                    x = obj.get("x", 0)
                    y = obj.get("y", 0)
                    w = obj.get("w", 0)
                    h = obj.get("h", 0)

                    bbox = BoundingBox(
                        x_min=x,
                        y_min=y,
                        x_max=x + w,
                        y_max=y + h,
                    )

                    if bbox.area < self.config.min_object_size:
                        continue

                    annotation = ObjectAnnotation(
                        label=matched_label,
                        bbox=bbox,
                        attributes={
                            "all_names": names,
                            "synsets": synsets,
                            "object_id": obj.get("object_id", ""),
                        }
                    )
                    matching_objects.append(annotation)

            if not matching_objects:
                continue

            # Get image metadata
            img_meta = image_data.get(image_id, {})
            url = img_meta.get("url", "")
            width = img_meta.get("width", 0)
            height = img_meta.get("height", 0)
            filename = f"{image_id}.jpg"

            record = ImageRecord(
                image_id=str(image_id),
                filename=filename,
                url=url,
                width=width,
                height=height,
                objects=matching_objects,
                source="visual_genome",
            )

            results.append(record)

        return results

    def _scrape_via_api(self) -> List[ImageRecord]:
        """Scrape using Visual Genome API (limited functionality)."""
        logger.info("Using Visual Genome API for scraping...")
        results = []

        # The VG API is limited, so we'll use a different approach
        # First, get all images
        try:
            # Get image IDs page by page
            page = 1
            all_image_ids = []

            while len(all_image_ids) < self.config.max_images * 10:  # oversample
                url = f"{self.config.vg_api_base}/images/all?page={page}"
                response = requests.get(url, timeout=30)
                if response.status_code != 200:
                    break
                data = response.json()
                if not data:
                    break
                all_image_ids.extend([img["id"] for img in data])
                page += 1

                if page > 20:  # limit API calls
                    break

            logger.info(f"Retrieved {len(all_image_ids)} image IDs from API")

            # Check objects for each image
            for img_id in tqdm(all_image_ids[:self.config.max_images * 5], desc="Checking images via API"):
                if len(results) >= self.config.max_images:
                    break

                try:
                    obj_url = f"{self.config.vg_api_base}/images/{img_id}/objects"
                    response = requests.get(obj_url, timeout=15)
                    if response.status_code != 200:
                        continue

                    obj_data = response.json()
                    matching_objects = []

                    for obj in obj_data.get("objects", []):
                        names = obj.get("names", [])
                        for name in names:
                            if self._matches_keyword(name):
                                bbox = BoundingBox(
                                    x_min=obj.get("x", 0),
                                    y_min=obj.get("y", 0),
                                    x_max=obj.get("x", 0) + obj.get("w", 0),
                                    y_max=obj.get("y", 0) + obj.get("h", 0),
                                )
                                annotation = ObjectAnnotation(
                                    label=name,
                                    bbox=bbox,
                                )
                                matching_objects.append(annotation)
                                break

                    if matching_objects:
                        # Get image info
                        img_url = f"{self.config.vg_api_base}/images/{img_id}"
                        img_response = requests.get(img_url, timeout=15)
                        img_info = img_response.json() if img_response.status_code == 200 else {}

                        record = ImageRecord(
                            image_id=str(img_id),
                            filename=f"{img_id}.jpg",
                            url=img_info.get("url", ""),
                            width=img_info.get("width", 0),
                            height=img_info.get("height", 0),
                            objects=matching_objects,
                            source="visual_genome_api",
                        )
                        results.append(record)

                except Exception as e:
                    logger.debug(f"Error processing image {img_id}: {e}")
                    continue

        except Exception as e:
            logger.error(f"API scraping failed: {e}")

        return results

    def _download_matched_images(self, records: List[ImageRecord]):
        """Download images for matched records using thread pool."""
        if not self.config.download_images:
            return

        logger.info(f"Downloading {len(records)} images...")

        def download_one(record: ImageRecord) -> Tuple[str, bool]:
            if not record.url:
                return record.image_id, False
            save_path = self.images_dir / record.filename
            if save_path.exists():
                return record.image_id, True
            return record.image_id, self._download_image(record.url, save_path)

        success_count = 0
        with ThreadPoolExecutor(max_workers=self.config.num_workers) as executor:
            futures = {executor.submit(download_one, rec): rec for rec in records}
            for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading images"):
                img_id, success = future.result()
                if success:
                    success_count += 1

        logger.info(f"Downloaded {success_count}/{len(records)} images")

    def scrape(self) -> List[ImageRecord]:
        """Main scraping entry point for Visual Genome."""
        # Try local data first
        objects_path = self._get_data_path("objects")

        if objects_path and objects_path.exists():
            self.results = self._scrape_from_local_objects()
        else:
            logger.info("Local VG data not available, trying API...")
            self.results = self._scrape_via_api()

        # Limit results
        self.results = self.results[:self.config.max_images]

        # Download images
        self._download_matched_images(self.results)

        # Save annotations
        if self.config.save_annotations:
            for record in self.results:
                self.save_per_image_annotation(record)

        logger.info(f"Scraped {len(self.results)} images from Visual Genome")
        self.save_results()
        return self.results


# ─── Factory & Main Interface ────────────────────────────────────────────────

class DatasetScraper:
    """Factory class for creating dataset-specific scrapers."""

    SCRAPERS = {
        DatasetType.PASCAL_VOC: PascalVOCScraper,
        DatasetType.VISUAL_GENOME: VisualGenomeScraper,
    }

    @staticmethod
    def create(config: ScrapingConfig) -> BaseScraper:
        scraper_class = DatasetScraper.SCRAPERS.get(config.dataset_type)
        if scraper_class is None:
            raise ValueError(f"Unsupported dataset type: {config.dataset_type}")
        return scraper_class(config)

    @staticmethod
    def scrape(
        dataset_type: str,
        keywords: List[str],
        output_dir: str = "./scraped_data",
        max_images: int = 1000,
        **kwargs,
    ) -> List[ImageRecord]:
        """Convenience method for quick scraping."""
        dt = DatasetType(dataset_type)
        config = ScrapingConfig(
            dataset_type=dt,
            keywords=keywords,
            output_dir=output_dir,
            max_images=max_images,
            **kwargs,
        )
        scraper = DatasetScraper.create(config)
        return scraper.scrape()


# ─── CLI Interface ───────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Keyword-based scraper for PASCAL VOC and Visual Genome datasets"
    )
    parser.add_argument(
        "--dataset",
        type=str,
        required=True,
        choices=["pascal_voc", "visual_genome"],
        help="Dataset to scrape",
    )
    parser.add_argument(
        "--keywords",
        type=str,
        nargs="+",
        required=True,
        help="Keywords to search for (e.g., 'dog cat person')",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./scraped_data",
        help="Output directory",
    )
    parser.add_argument(
        "--max-images",
        type=int,
        default=1000,
        help="Maximum number of images to scrape",
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=4,
        help="Number of download workers",
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Don't download images, only extract annotations",
    )
    parser.add_argument(
        "--voc-root",
        type=str,
        default="",
        help="Path to local PASCAL VOC dataset root",
    )
    parser.add_argument(
        "--voc-year",
        type=str,
        default="2012",
        choices=["2007", "2012"],
        help="PASCAL VOC year",
    )
    parser.add_argument(
        "--vg-data-dir",
        type=str,
        default="",
        help="Path to local Visual Genome data directory",
    )
    parser.add_argument(
        "--min-object-size",
        type=int,
        default=0,
        help="Minimum bounding box area to include",
    )

    args = parser.parse_args()

    config = ScrapingConfig(
        dataset_type=DatasetType(args.dataset),
        keywords=args.keywords,
        output_dir=args.output_dir,
        max_images=args.max_images,
        num_workers=args.num_workers,
        download_images=not args.no_download,
        voc_root=args.voc_root,
        voc_year=args.voc_year,
        vg_data_dir=args.vg_data_dir,
        min_object_size=args.min_object_size,
    )

    scraper = DatasetScraper.create(config)
    results = scraper.scrape()

    # Print summary
    print("\n" + "=" * 60)
    print(f"SCRAPING SUMMARY")
    print("=" * 60)
    print(f"Dataset:      {config.dataset_type.value}")
    print(f"Keywords:     {', '.join(args.keywords)}")
    print(f"Images found: {len(results)}")

    if results:
        # Count objects per keyword
        keyword_counts = {}
        for record in results:
            for obj in record.objects:
                label = obj.label.lower()
                keyword_counts[label] = keyword_counts.get(label, 0) + 1

        print(f"\nObject counts per label:")
        for label, count in sorted(keyword_counts.items(), key=lambda x: -x[1]):
            print(f"  {label}: {count}")

    print(f"\nOutput directory: {config.output_dir}/{config.dataset_type.value}")
    print("=" * 60)


# ─── Usage Examples ──────────────────────────────────────────────────────────

def example_usage():
    """Example usage of the scraping library."""

    # Example 1: Scrape PASCAL VOC for dogs and cats
    print("--- Example 1: PASCAL VOC ---")
    voc_config = ScrapingConfig(
        dataset_type=DatasetType.PASCAL_VOC,
        keywords=["dog", "cat"],
        output_dir="./my_scraped_data",
        max_images=100,
        voc_root="/path/to/VOCdevkit",  # set your local path
        voc_year="2012",
    )
    voc_scraper = DatasetScraper.create(voc_config)
    voc_results = voc_scraper.scrape()
    print(f"Found {len(voc_results)} images with dogs/cats in VOC")

    # Example 2: Scrape Visual Genome for people and vehicles
    print("\n--- Example 2: Visual Genome ---")
    vg_config = ScrapingConfig(
        dataset_type=DatasetType.VISUAL_GENOME,
        keywords=["person", "car", "bus", "truck"],
        output_dir="./my_scraped_data",
        max_images=200,
        vg_data_dir="/path/to/visual_genome",  # set your local path
        download_images=True,
        num_workers=8,
    )
    vg_scraper = DatasetScraper.create(vg_config)
    vg_results = vg_scraper.scrape()
    print(f"Found {len(vg_results)} images with people/vehicles in VG")

    # Example 3: Quick one-liner
    results = DatasetScraper.scrape(
        dataset_type="pascal_voc",
        keywords=["horse", "sheep", "cow"],
        max_images=50,
    )
    print(f"\nQuick scrape found {len(results)} images")


if __name__ == "__main__":
    main()
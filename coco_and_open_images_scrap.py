"""
Dataset Scraping Script for COCO and Open Images datasets.
Downloads images and annotations based on keyword/category search.
used for coco dataset and open images
uv run coco_and_open_images_scrap.py --source coco --keywords cat --max-images 50 --output-dir open_op #for coco
uv run coco_and_open_images_scrap.py --source open_images --keywords cat --max-images 50 --output-dir open_op # for open images
"""

import os
import sys
import json
import requests
import argparse
import csv
import zipfile
from enum import Enum
from pathlib import Path
from typing import List, Optional, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import urllib.request


# ============================================================
# ENUMS & CONFIG
# ============================================================

class DatasetSource(Enum):
    COCO = "coco"
    OPEN_IMAGES = "open_images"


class Config:
    """Configuration for dataset paths and URLs."""

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    # COCO Dataset URLs
    COCO_ANNOTATIONS_URL = "http://images.cocodataset.org/annotations/annotations_trainval2017.zip"
    COCO_INSTANCES_TRAIN_FILE = "annotations/instances_train2017.json"
    COCO_INSTANCES_VAL_FILE = "annotations/instances_val2017.json"

    # Open Images Dataset URLs
    OID_CLASS_DESCRIPTIONS_URL = "https://storage.googleapis.com/openimages/v7/oidv7-class-descriptions-boxable.csv"
    OID_VALIDATION_ANNOTATIONS_URL = "https://storage.googleapis.com/openimages/v5/validation-annotations-bbox.csv"
    OID_TRAIN_ANNOTATIONS_URL = "https://storage.googleapis.com/openimages/v6/oidv6-train-annotations-bbox.csv"
    OID_TEST_ANNOTATIONS_URL = "https://storage.googleapis.com/openimages/v5/test-annotations-bbox.csv"
    OID_IMAGE_BASE_URL = "https://open-images-dataset.s3.amazonaws.com"


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def download_file(url: str, dest_path: str, desc: str = None) -> str:
    """Download a file with progress bar and proper headers."""
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
        print(f"[SKIP] Already exists: {dest_path}")
        return dest_path

    print(f"[DOWNLOAD] {desc or url}")
    response = requests.get(url, headers=Config.HEADERS, stream=True)
    response.raise_for_status()
    total_size = int(response.headers.get("content-length", 0))

    with open(dest_path, "wb") as f, tqdm(
        total=total_size, unit="B", unit_scale=True, desc=desc or "Downloading"
    ) as pbar:
        for chunk in response.iter_content(chunk_size=65536):
            f.write(chunk)
            pbar.update(len(chunk))

    return dest_path


def download_single_image(args_tuple) -> Optional[str]:
    """Download a single image with headers and error handling."""
    url, dest_path = args_tuple
    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
        return dest_path

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    try:
        req = urllib.request.Request(url, headers=Config.HEADERS)
        with urllib.request.urlopen(req, timeout=20) as response, open(dest_path, "wb") as out_file:
            out_file.write(response.read())
        return dest_path
    except Exception:
        if os.path.exists(dest_path):
            os.remove(dest_path)
        return None


def download_images_parallel(
    image_tasks: List[tuple],
    max_workers: int = 16,
    desc: str = "Downloading images"
) -> List[str]:
    """Download images in parallel using ThreadPoolExecutor."""
    results = []
    failed_count = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_single_image, task): task for task in image_tasks}
        with tqdm(total=len(futures), desc=desc) as pbar:
            for future in as_completed(futures):
                result = future.result()
                if result:
                    results.append(result)
                else:
                    failed_count += 1
                pbar.update(1)

    if failed_count > 0:
        print(f"[WARN] {failed_count}/{len(image_tasks)} images failed to download.")
    return results


# ============================================================
# COCO SCRAPER
# ============================================================

class COCOScraper:
    """Scraper for COCO dataset based on keyword/category."""

    def __init__(self, output_dir: str = "./data/coco", annotations_dir: str = "./cache/coco_annotations"):
        self.output_dir = Path(output_dir)
        self.annotations_dir = Path(annotations_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.annotations_dir.mkdir(parents=True, exist_ok=True)
        self.categories = {}
        self.annotations_data = None

    def _download_annotations(self, split: str = "train") -> str:
        """Download and extract COCO annotations."""
        zip_path = self.annotations_dir / "annotations_trainval2017.zip"
        rel_json = Config.COCO_INSTANCES_TRAIN_FILE if split == "train" else Config.COCO_INSTANCES_VAL_FILE
        json_file = self.annotations_dir / rel_json

        if json_file.exists():
            print(f"[SKIP] Annotations already exist: {json_file}")
            return str(json_file)

        if not zip_path.exists():
            download_file(
                Config.COCO_ANNOTATIONS_URL,
                str(zip_path),
                desc="COCO Annotations"
            )

        print("[EXTRACT] Extracting annotations...")
        with zipfile.ZipFile(str(zip_path), "r") as zip_ref:
            zip_ref.extractall(str(self.annotations_dir))

        return str(json_file)

    def _load_annotations(self, split: str = "train"):
        """Load COCO annotation JSON."""
        if self.annotations_data is not None:
            return

        json_path = self._download_annotations(split)
        print(f"[LOAD] Loading annotations from {json_path}...")
        with open(json_path, "r", encoding="utf-8") as f:
            self.annotations_data = json.load(f)

        self.categories = {
            cat["id"]: cat["name"] for cat in self.annotations_data["categories"]
        }

    def list_categories(self) -> List[str]:
        """List all available COCO categories."""
        self._load_annotations("train")
        return sorted(self.categories.values())

    def search_categories(self, keyword: str, exact: bool = False) -> List[Dict]:
        """Search categories matching a keyword."""
        self._load_annotations("train")
        keyword_lower = keyword.strip().lower()
        matches = []
        for cat in self.annotations_data["categories"]:
            name = cat["name"].lower()
            if exact and name == keyword_lower:
                matches.append(cat)
            elif not exact and keyword_lower in name:
                matches.append(cat)

        if not exact and len(matches) > 1:
            exact_matches = [c for c in matches if c["name"].lower() == keyword_lower]
            if exact_matches:
                matches = exact_matches
        return matches

    def scrape(
        self,
        keywords: List[str],
        split: str = "train",
        max_images: int = 100,
        max_workers: int = 16,
        save_annotations: bool = True,
        exact: bool = False
    ) -> Dict:
        self._load_annotations(split)
        results = {}

        for keyword in keywords:
            print(f"\n{'='*60}")
            print(f"[SEARCH] Keyword: '{keyword}' in COCO {split}")
            print(f"{'='*60}")

            matching_cats = self.search_categories(keyword, exact=exact)
            if not matching_cats:
                print(f"[WARN] No categories found matching '{keyword}'")
                results[keyword] = {"status": "no_match", "images": []}
                continue

            print(f"[FOUND] Matching categories: {[c['name'] for c in matching_cats]}")
            cat_ids = [c["id"] for c in matching_cats]

            matching_annotations = [
                ann for ann in self.annotations_data["annotations"]
                if ann["category_id"] in cat_ids
            ]

            image_ids = list(dict.fromkeys(ann["image_id"] for ann in matching_annotations))
            print(f"[INFO] Found {len(image_ids)} images matching '{keyword}'")
            image_ids = image_ids[:max_images]

            image_lookup = {img["id"]: img for img in self.annotations_data["images"]}
            keyword_clean = keyword.strip().replace(" ", "_")
            keyword_dir = self.output_dir / keyword_clean / split
            keyword_dir.mkdir(parents=True, exist_ok=True)

            download_tasks = []
            image_metadata = []

            for img_id in image_ids:
                if img_id not in image_lookup:
                    continue
                img_info = image_lookup[img_id]
                url = img_info["coco_url"]
                filename = img_info["file_name"]
                dest_path = str(keyword_dir / filename)
                download_tasks.append((url, dest_path))

                img_anns = [
                    {
                        "category": self.categories.get(ann["category_id"], "unknown"),
                        "category_id": ann["category_id"],
                        "bbox": ann["bbox"],
                        "area": ann["area"],
                        "iscrowd": ann.get("iscrowd", 0)
                    }
                    for ann in matching_annotations if ann["image_id"] == img_id
                ]
                image_metadata.append({
                    "image_id": img_id,
                    "filename": filename,
                    "url": url,
                    "width": img_info.get("width"),
                    "height": img_info.get("height"),
                    "annotations": img_anns
                })

            downloaded = download_images_parallel(
                download_tasks, max_workers=max_workers,
                desc=f"COCO - {keyword}"
            )

            if save_annotations:
                ann_file = keyword_dir / "annotations.json"
                with open(ann_file, "w", encoding="utf-8") as f:
                    json.dump({
                        "keyword": keyword,
                        "source": "coco",
                        "split": split,
                        "categories": matching_cats,
                        "total_images": len(image_metadata),
                        "images": image_metadata
                    }, f, indent=2)
                print(f"[SAVE] Annotations saved to {ann_file}")

            results[keyword] = {
                "status": "success",
                "categories": [c["name"] for c in matching_cats],
                "total_found": len(image_ids),
                "downloaded": len(downloaded),
                "output_dir": str(keyword_dir),
            }

        return results


# ============================================================
# OPEN IMAGES SCRAPER
# ============================================================

class OpenImagesScraper:
    """Scraper for Open Images dataset based on keyword/category."""

    def __init__(self, output_dir: str = "./data/open_images", cache_dir: str = "./cache/open_images"):
        self.output_dir = Path(output_dir)
        self.cache_dir = Path(cache_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.class_descriptions = {}
        self.class_name_to_id = {}

    def _load_class_descriptions(self):
        """Download and load Open Images class descriptions."""
        if self.class_descriptions:
            return

        csv_path = self.cache_dir / "oidv7-class-descriptions-boxable.csv"
        if not csv_path.exists():
            download_file(
                Config.OID_CLASS_DESCRIPTIONS_URL,
                str(csv_path),
                desc="Class Descriptions"
            )

        print("[LOAD] Loading class descriptions...")
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 2:
                    label_id = row[0].strip()
                    label_name = row[1].strip()
                    self.class_descriptions[label_id] = label_name
                    self.class_name_to_id[label_name.lower()] = label_id

        print(f"[INFO] Loaded {len(self.class_descriptions)} classes")

    def list_categories(self) -> List[str]:
        """List all available Open Images categories."""
        self._load_class_descriptions()
        return sorted(self.class_descriptions.values())

    def search_categories(self, keyword: str, exact: bool = False) -> List[Dict]:
        """Search categories matching a keyword."""
        self._load_class_descriptions()
        keyword_lower = keyword.strip().lower()
        matches = []

        for label_id, label_name in self.class_descriptions.items():
            name_lower = label_name.lower()
            if exact and name_lower == keyword_lower:
                matches.append({"id": label_id, "name": label_name})
            elif not exact and keyword_lower in name_lower:
                matches.append({"id": label_id, "name": label_name})

        # Prefer exact match if found among multiple matches
        if not exact and len(matches) > 1:
            exact_matches = [m for m in matches if m["name"].lower() == keyword_lower]
            if exact_matches:
                matches = exact_matches

        return matches

    def _load_annotations(self, split: str = "validation") -> str:
        """Download correct annotation CSV for Open Images."""
        url_map = {
            "validation": (Config.OID_VALIDATION_ANNOTATIONS_URL, "validation-annotations-bbox.csv"),
            "train": (Config.OID_TRAIN_ANNOTATIONS_URL, "oidv6-train-annotations-bbox.csv"),
            "test": (Config.OID_TEST_ANNOTATIONS_URL, "test-annotations-bbox.csv"),
        }

        if split not in url_map:
            raise ValueError(f"Invalid split '{split}'. Choose from: {list(url_map.keys())}")

        url, filename = url_map[split]
        csv_path = self.cache_dir / filename

        if not csv_path.exists():
            download_file(url, str(csv_path), desc=f"OID {split} annotations")

        return str(csv_path)

    def _get_image_url(self, image_id: str, split: str = "validation") -> str:
        """Construct download URL for Open Images AWS S3 bucket."""
        return f"{Config.OID_IMAGE_BASE_URL}/{split}/{image_id}.jpg"

    def scrape(
        self,
        keywords: List[str],
        split: str = "validation",
        max_images: int = 100,
        max_workers: int = 16,
        save_annotations: bool = True,
        exact: bool = False
    ) -> Dict:
        self._load_class_descriptions()
        results = {}

        for keyword in keywords:
            print(f"\n{'='*60}")
            print(f"[SEARCH] Keyword: '{keyword}' in Open Images {split}")
            print(f"{'='*60}")

            matching_cats = self.search_categories(keyword, exact=exact)
            if not matching_cats:
                print(f"[WARN] No categories found matching '{keyword}'")
                results[keyword] = {"status": "no_match", "images": []}
                continue

            print(f"[FOUND] Matching categories: {[c['name'] for c in matching_cats]}")
            label_ids = set(c["id"] for c in matching_cats)

            ann_csv_path = self._load_annotations(split)
            print(f"[LOAD] Scanning annotations for matching images...")

            image_annotations = {}
            with open(ann_csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    label = row.get("LabelName")
                    if label in label_ids:
                        img_id = row["ImageID"]
                        if img_id not in image_annotations:
                            image_annotations[img_id] = []
                        image_annotations[img_id].append({
                            "label_id": label,
                            "label_name": self.class_descriptions.get(label, "unknown"),
                            "confidence": row.get("Confidence", "1"),
                            "bbox": [
                                float(row.get("XMin", 0)),
                                float(row.get("YMin", 0)),
                                float(row.get("XMax", 0)),
                                float(row.get("YMax", 0)),
                            ],
                            "is_occluded": row.get("IsOccluded", "0"),
                            "is_truncated": row.get("IsTruncated", "0"),
                            "is_group_of": row.get("IsGroupOf", "0"),
                        })

            image_ids = list(image_annotations.keys())
            print(f"[INFO] Found {len(image_ids)} images with '{keyword}'")
            image_ids = image_ids[:max_images]

            keyword_clean = keyword.strip().replace(" ", "_")
            keyword_dir = self.output_dir / keyword_clean / split
            keyword_dir.mkdir(parents=True, exist_ok=True)

            download_tasks = []
            image_metadata = []

            for img_id in image_ids:
                url = self._get_image_url(img_id, split)
                filename = f"{img_id}.jpg"
                dest_path = str(keyword_dir / filename)
                download_tasks.append((url, dest_path))

                image_metadata.append({
                    "image_id": img_id,
                    "filename": filename,
                    "url": url,
                    "annotations": image_annotations.get(img_id, [])
                })

            downloaded = download_images_parallel(
                download_tasks, max_workers=max_workers,
                desc=f"Open Images - {keyword}"
            )

            if save_annotations:
                ann_file = keyword_dir / "annotations.json"
                with open(ann_file, "w", encoding="utf-8") as f:
                    json.dump({
                        "keyword": keyword,
                        "source": "open_images",
                        "split": split,
                        "categories": matching_cats,
                        "total_images": len(image_metadata),
                        "images": image_metadata
                    }, f, indent=2)
                print(f"[SAVE] Annotations saved to {ann_file}")

            results[keyword] = {
                "status": "success",
                "categories": [c["name"] for c in matching_cats],
                "total_found": len(image_annotations),
                "downloaded": len(downloaded),
                "output_dir": str(keyword_dir),
            }

        return results


# ============================================================
# UNIFIED SCRAPER
# ============================================================

class DatasetScraper:
    """Unified scraper interface for multiple dataset sources."""

    def __init__(self, output_dir: str = "./data", cache_dir: str = "./cache"):
        self.output_dir = output_dir
        self.cache_dir = cache_dir
        self._scrapers = {
            DatasetSource.COCO: COCOScraper(
                output_dir=os.path.join(output_dir, "coco"),
                annotations_dir=os.path.join(cache_dir, "coco_annotations")
            ),
            DatasetSource.OPEN_IMAGES: OpenImagesScraper(
                output_dir=os.path.join(output_dir, "open_images"),
                cache_dir=os.path.join(cache_dir, "open_images")
            ),
        }

    def scrape(
        self,
        source: DatasetSource,
        keywords: List[str],
        split: str = "train",
        max_images: int = 100,
        max_workers: int = 16,
        save_annotations: bool = True,
        exact: bool = False
    ) -> Dict:
        scraper = self._scrapers[source]
        return scraper.scrape(
            keywords=keywords,
            split=split,
            max_images=max_images,
            max_workers=max_workers,
            save_annotations=save_annotations,
            exact=exact
        )

    def scrape_all_sources(
        self,
        keywords: List[str],
        max_images: int = 100,
        max_workers: int = 16,
        exact: bool = False
    ) -> Dict:
        all_results = {}
        for source in DatasetSource:
            print(f"\n{'#'*60}")
            print(f"# SOURCE: {source.value.upper()}")
            print(f"{'#'*60}")

            split = "train" if source == DatasetSource.COCO else "validation"
            try:
                all_results[source.value] = self.scrape(
                    source=source,
                    keywords=keywords,
                    split=split,
                    max_images=max_images,
                    max_workers=max_workers,
                    exact=exact
                )
            except Exception as e:
                print(f"[ERROR] Failed scraping {source.value}: {e}")
                all_results[source.value] = {"error": str(e)}

        return all_results

    def list_categories(self, source: DatasetSource) -> List[str]:
        return self._scrapers[source].list_categories()

    def search_categories(self, source: DatasetSource, keyword: str) -> List[Dict]:
        return self._scrapers[source].search_categories(keyword)


# ============================================================
# CLI INTERFACE
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Scrape images from COCO and Open Images datasets by keyword"
    )

    parser.add_argument(
        "--source",
        type=str,
        choices=["coco", "open_images", "all"],
        default="coco",
        help="Dataset source to scrape from (default: coco)"
    )
    parser.add_argument(
        "--keywords",
        nargs="+",
        type=str,
        help="Keywords/categories to search for"
    )
    parser.add_argument(
        "--split",
        type=str,
        default=None,
        help="Dataset split (train/val for COCO; validation/train/test for Open Images)"
    )
    parser.add_argument(
        "--max-images",
        type=int,
        default=50,
        help="Maximum images to download per keyword (default: 50)"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=16,
        help="Number of parallel download threads (default: 16)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./data",
        help="Output directory for downloaded images"
    )
    parser.add_argument(
        "--cache-dir",
        type=str,
        default="./cache",
        help="Cache directory for annotations"
    )
    parser.add_argument(
        "--exact",
        action="store_true",
        help="Strict exact match for category names"
    )
    parser.add_argument(
        "--list-categories",
        type=str,
        choices=["coco", "open_images"],
        help="List all available categories for a dataset"
    )
    parser.add_argument(
        "--search-category",
        type=str,
        choices=["coco", "open_images"],
        help="Search category names matching keywords"
    )
    parser.add_argument(
        "--no-annotations",
        action="store_true",
        help="Skip saving annotation JSON files"
    )

    args = parser.parse_args()
    scraper = DatasetScraper(output_dir=args.output_dir, cache_dir=args.cache_dir)

    # 1. List categories
    if args.list_categories:
        source = DatasetSource(args.list_categories)
        categories = scraper.list_categories(source)
        print(f"\nAvailable categories in {source.value.upper()} ({len(categories)} total):")
        for i, cat in enumerate(categories, 1):
            print(f"  {i:4d}. {cat}")
        return

    # 2. Search categories
    if args.search_category:
        if not args.keywords:
            print("[ERROR] Please provide --keywords to search")
            return
        source = DatasetSource(args.search_category)
        for kw in args.keywords:
            matches = scraper.search_categories(source, kw)
            print(f"\nCategories matching '{kw}' in {source.value.upper()}:")
            for m in matches:
                print(f"  - {m['name']} (ID: {m['id']})")
        return

    # 3. Scrape images
    if not args.keywords:
        parser.print_help()
        print("\n[ERROR] --keywords is required for scraping")
        return

    if args.source == "all":
        results = scraper.scrape_all_sources(
            keywords=args.keywords,
            max_images=args.max_images,
            max_workers=args.max_workers,
            exact=args.exact
        )
    else:
        source = DatasetSource(args.source)
        default_split = "train" if source == DatasetSource.COCO else "validation"
        split = args.split or default_split

        results = scraper.scrape(
            source=source,
            keywords=args.keywords,
            split=split,
            max_images=args.max_images,
            max_workers=args.max_workers,
            save_annotations=not args.no_annotations,
            exact=args.exact
        )

    print(f"\n{'='*60}")
    print("DOWNLOAD COMPLETE")
    print(f"{'='*60}")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
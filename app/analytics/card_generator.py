"""
Auto-Generated Dataset Card Generator.
Generates comprehensive dataset documentation using available metadata.
"""
from typing import Dict, Any, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class CardGenerator:
    """
    Generate comprehensive dataset cards with structured documentation.
    
    Creates markdown-compatible dataset cards following best practices.
    """
    
    def generate_card(self, dataset: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a complete dataset card.
        
        Returns structured sections that can be rendered or downloaded.
        """
        name = dataset.get('canonical_name', 'Unknown Dataset')
        description = dataset.get('description', '')
        
        # Generate all sections
        sections = {
            'overview': self._generate_overview(dataset),
            'dataset_info': self._generate_dataset_info(dataset),
            'usage': self._generate_usage(dataset),
            'considerations': self._generate_considerations(dataset),
            'citation': self._generate_citation(dataset)
        }
        
        # Generate full markdown
        markdown = self._generate_markdown(name, sections)
        
        return {
            'name': name,
            'sections': sections,
            'markdown': markdown,
            'generated_at': datetime.now().isoformat(),
            'word_count': len(markdown.split())
        }
    
    def _generate_overview(self, dataset: Dict[str, Any]) -> Dict[str, Any]:
        """Generate overview section."""
        description = dataset.get('description', '')
        domain = dataset.get('domain', 'General')
        modality = dataset.get('modality', 'Mixed')
        task = dataset.get('task', 'Various')
        
        # Generate summary if description is too short
        if len(description) < 50:
            summary = f"This is a {modality} dataset in the {domain} domain, designed for {task} tasks."
        else:
            summary = description[:500] + ('...' if len(description) > 500 else '')
        
        return {
            'title': 'Overview',
            'summary': summary,
            'highlights': self._generate_highlights(dataset)
        }
    
    def _generate_highlights(self, dataset: Dict[str, Any]) -> list:
        """Generate key dataset highlights."""
        highlights = []
        metadata = dataset.get('source', {}).get('source_metadata', {})
        
        # Size highlight
        samples = dataset.get('size', {}).get('samples')
        if samples:
            if samples >= 1000000:
                highlights.append(f"Large-scale dataset with {samples:,} samples")
            elif samples >= 10000:
                highlights.append(f"Medium-sized dataset with {samples:,} samples")
            else:
                highlights.append(f"Contains {samples:,} samples")
        
        # Popularity highlight
        downloads = metadata.get('downloads', 0) or 0
        if downloads >= 100000:
            highlights.append(f"Highly popular with {downloads:,}+ downloads")
        elif downloads >= 10000:
            highlights.append(f"Well-established with {downloads:,}+ downloads")
        
        # License highlight
        license = dataset.get('license', '')
        if license:
            if 'mit' in license.lower() or 'apache' in license.lower():
                highlights.append(f"Commercially friendly ({license})")
            else:
                highlights.append(f"Licensed under {license}")
        
        # Platform highlight
        platform = dataset.get('source', {}).get('platform', '')
        if platform:
            highlights.append(f"Hosted on {platform.title()}")
        
        return highlights[:5]  # Limit to 5 highlights
    
    def _generate_dataset_info(self, dataset: Dict[str, Any]) -> Dict[str, Any]:
        """Generate dataset information section."""
        metadata = dataset.get('source', {}).get('source_metadata', {})
        size_info = dataset.get('size', {})
        
        return {
            'title': 'Dataset Information',
            'fields': {
                'Domain': dataset.get('domain', 'Not specified'),
                'Modality': dataset.get('modality', 'Not specified'),
                'Task': dataset.get('task', 'Not specified'),
                'Samples': f"{size_info.get('samples', 0):,}" if size_info.get('samples') else 'Unknown',
                'Features': size_info.get('features', 'Not specified'),
                'Size': self._format_bytes(size_info.get('bytes')),
                'License': dataset.get('license', 'Not specified'),
                'Version': dataset.get('version', '1.0'),
                'Last Updated': metadata.get('last_modified', 'Unknown'),
                'Platform': dataset.get('source', {}).get('platform', 'Unknown')
            }
        }
    
    def _generate_usage(self, dataset: Dict[str, Any]) -> Dict[str, Any]:
        """Generate usage information."""
        platform = dataset.get('source', {}).get('platform', '')
        platform_id = dataset.get('source', {}).get('platform_id', '')
        modality = dataset.get('modality', '').lower()
        
        # Generate code snippets based on platform
        code_snippets = []
        
        if platform == 'huggingface' and platform_id:
            code_snippets.append({
                'language': 'python',
                'title': 'Load with Hugging Face datasets',
                'code': f'''from datasets import load_dataset

# Load the dataset
dataset = load_dataset("{platform_id}")

# Access the data
train_data = dataset["train"]
print(f"Dataset size: {{len(train_data)}}")'''
            })
        
        if platform == 'kaggle' and platform_id:
            code_snippets.append({
                'language': 'python',
                'title': 'Download with Kaggle API',
                'code': f'''import kaggle

# Download the dataset
kaggle.api.dataset_download_files('{platform_id}', unzip=True)'''
            })
        
        # Add general loading based on modality
        if 'image' in modality:
            code_snippets.append({
                'language': 'python',
                'title': 'Load images with PyTorch',
                'code': '''from torchvision import datasets, transforms

transform = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
])

dataset = datasets.ImageFolder('path/to/data', transform=transform)'''
            })
        
        if 'text' in modality:
            code_snippets.append({
                'language': 'python',
                'title': 'Process text data',
                'code': '''from transformers import AutoTokenizer

tokenizer = AutoTokenizer.from_pretrained("bert-base-uncased")

def preprocess(examples):
    return tokenizer(examples["text"], truncation=True, padding=True)

tokenized = dataset.map(preprocess, batched=True)'''
            })
        
        # Recommended use cases
        use_cases = self._generate_use_cases(dataset)
        
        return {
            'title': 'Usage',
            'code_snippets': code_snippets,
            'use_cases': use_cases,
            'access_url': metadata.get('url') if (metadata := dataset.get('source', {}).get('source_metadata', {})) else None
        }
    
    def _generate_use_cases(self, dataset: Dict[str, Any]) -> list:
        """Generate recommended use cases."""
        task = (dataset.get('task') or '').lower()
        modality = (dataset.get('modality') or '').lower()
        domain = (dataset.get('domain') or '').lower()
        
        use_cases = []
        
        # Task-based use cases
        if 'classification' in task:
            use_cases.append('Train classification models')
            use_cases.append('Benchmark classifier performance')
        if 'detection' in task:
            use_cases.append('Object detection model training')
            use_cases.append('Localization tasks')
        if 'question' in task or 'qa' in task:
            use_cases.append('Question answering systems')
            use_cases.append('RAG pipeline development')
        if 'generation' in task:
            use_cases.append('Text/image generation fine-tuning')
        
        # Domain-based use cases
        if 'medical' in domain or 'health' in domain:
            use_cases.append('Healthcare AI research')
        if 'finance' in domain:
            use_cases.append('Financial analysis models')
        
        # Modality-based use cases
        if 'image' in modality:
            use_cases.append('Computer vision research')
            use_cases.append('Transfer learning experiments')
        if 'text' in modality:
            use_cases.append('NLP model development')
            use_cases.append('Language understanding tasks')
        if 'audio' in modality:
            use_cases.append('Speech recognition')
            use_cases.append('Audio classification')
        
        return list(set(use_cases))[:6]  # Deduplicate and limit
    
    def _generate_considerations(self, dataset: Dict[str, Any]) -> Dict[str, Any]:
        """Generate ethical and practical considerations."""
        domain = (dataset.get('domain') or '').lower()
        modality = (dataset.get('modality') or '').lower()
        
        ethical = []
        limitations = []
        
        # Domain-specific ethics
        if 'medical' in domain or 'health' in domain:
            ethical.append('Ensure HIPAA compliance and patient privacy')
            ethical.append('Validate results with medical professionals')
            limitations.append('May not generalize to all patient populations')
        
        if 'face' in modality or 'person' in domain:
            ethical.append('Consider demographic representation and fairness')
            ethical.append('Obtain proper consent for facial data usage')
            ethical.append('Be aware of potential surveillance applications')
        
        # General considerations
        ethical.append('Verify licensing compliance for your use case')
        ethical.append('Credit the original dataset creators')
        
        # Size-based limitations
        samples = dataset.get('size', {}).get('samples', 0) or 0
        if samples < 1000:
            limitations.append('Small dataset size may limit model generalization')
        
        # Modality limitations
        if 'image' in modality:
            limitations.append('May require data augmentation for best results')
        if 'text' in modality:
            limitations.append('Check for language/domain coverage limitations')
        
        return {
            'title': 'Considerations',
            'ethical': ethical[:5],
            'limitations': limitations[:5],
            'best_practices': [
                'Split data properly for training/validation/test',
                'Monitor for data leakage',
                'Document your preprocessing steps',
                'Track model version alongside dataset version'
            ]
        }
    
    def _generate_citation(self, dataset: Dict[str, Any]) -> Dict[str, Any]:
        """Generate citation information."""
        name = dataset.get('canonical_name', 'Dataset')
        metadata = dataset.get('source', {}).get('source_metadata', {})
        author = metadata.get('author', 'Unknown')
        year = datetime.now().year
        platform = dataset.get('source', {}).get('platform', 'Unknown')
        url = metadata.get('url', '')
        
        bibtex = f'''@misc{{{name.replace('/', '_').replace('-', '_')}_{year},
  author = {{{author}}},
  title = {{{name}}},
  year = {{{year}}},
  publisher = {{{platform.title()}}},
  howpublished = {{\\url{{{url}}}}}
}}'''
        
        return {
            'title': 'Citation',
            'bibtex': bibtex,
            'apa': f"{author}. ({year}). {name}. {platform.title()}. {url}"
        }
    
    def _generate_markdown(self, name: str, sections: Dict[str, Any]) -> str:
        """Generate full markdown document."""
        lines = [f"# {name}", ""]
        
        # Overview
        overview = sections['overview']
        lines.append(f"## {overview['title']}")
        lines.append("")
        lines.append(overview['summary'])
        lines.append("")
        
        if overview['highlights']:
            lines.append("### Key Highlights")
            for h in overview['highlights']:
                lines.append(f"- {h}")
            lines.append("")
        
        # Dataset Info
        info = sections['dataset_info']
        lines.append(f"## {info['title']}")
        lines.append("")
        lines.append("| Property | Value |")
        lines.append("|----------|-------|")
        for key, value in info['fields'].items():
            lines.append(f"| {key} | {value} |")
        lines.append("")
        
        # Usage
        usage = sections['usage']
        lines.append(f"## {usage['title']}")
        lines.append("")
        
        if usage['use_cases']:
            lines.append("### Recommended Use Cases")
            for uc in usage['use_cases']:
                lines.append(f"- {uc}")
            lines.append("")
        
        for snippet in usage['code_snippets']:
            lines.append(f"### {snippet['title']}")
            lines.append("")
            lines.append(f"```{snippet['language']}")
            lines.append(snippet['code'])
            lines.append("```")
            lines.append("")
        
        # Considerations
        cons = sections['considerations']
        lines.append(f"## {cons['title']}")
        lines.append("")
        
        if cons['ethical']:
            lines.append("### Ethical Considerations")
            for e in cons['ethical']:
                lines.append(f"- {e}")
            lines.append("")
        
        if cons['limitations']:
            lines.append("### Limitations")
            for l in cons['limitations']:
                lines.append(f"- {l}")
            lines.append("")
        
        # Citation
        cite = sections['citation']
        lines.append(f"## {cite['title']}")
        lines.append("")
        lines.append("### BibTeX")
        lines.append("```bibtex")
        lines.append(cite['bibtex'])
        lines.append("```")
        lines.append("")
        lines.append("### APA")
        lines.append(cite['apa'])
        lines.append("")
        
        return "\n".join(lines)
    
    def _format_bytes(self, bytes_val: Optional[int]) -> str:
        """Format bytes to human readable."""
        if not bytes_val:
            return 'Unknown'
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_val < 1024:
                return f"{bytes_val:.1f} {unit}"
            bytes_val /= 1024
        return f"{bytes_val:.1f} PB"


# Singleton instance
card_generator = CardGenerator()

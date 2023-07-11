#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  3 14:16:49 2021

@author: muskrat
"""

import pandas as pd

pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)
# pd.set_option('display.max_colwidth', None)
# %%

species = pd.read_json('/media/muskrat/060A9B8E0A9B78FF/eco_data_copy/main_eco_data/final/species_prep.json')


# %%

# head = species.tail(n=1000)

# non['kingdom'] = np.where(non['species_type'].notna(), non['kingdom'] =='Plantae')

species.loc[species['species_type'].notna(), 'kingdom'] = 'Plantae'

species.loc[species['class'] == 'Mammalia', 'kingdom'] = 'Animalia'

species.loc[species['class'] == 'Amphibia', 'kingdom'] = 'Animalia'

species.loc[species['class'] == 'Reptilia', 'kingdom'] = 'Animalia'

species.loc[species['class'] == 'Aves', 'kingdom'] = 'Animalia'



# %%

tester = species['scientific_name'].str.split(' ', expand=True)

species['genus'] = tester[0]

# %%

fungi = species[(species['kingdom'] == 'Fungi')]

chromista = species[(species['kingdom'] == 'Chromista')]

bacteria = species[(species['kingdom'] == 'Bacteria')]

protozoa = species[(species['kingdom'] == 'Protozoa')]

virus = species[(species['kingdom'] == 'Viruses')]

animal = species[(species['kingdom'] == 'Animalia')]

plant = species[(species['kingdom'] == 'Plantae')]

none = species[(species['species_type'].notna())]

non_plant = plant[(plant['species_type'].isna())]

non_animal = animal[(animal['species_type'].isna())]

non_fungi = fungi[(fungi['species_type'].isna())]

neme = species[(species['species_type'].isna())]

# %%

species.loc[species['species_type'] == 'Graminoid', 'species_type'] = 'graminoid'

species.loc[species['species_type'] == 'Wildflower', 'species_type'] = 'wildflower'

species.loc[species['species_type'] == 'Tree', 'species_type'] = 'tree/shrub'

species.loc[species['species_type'] == 'Shrub', 'species_type'] = 'tree/shrub'

species.loc[species['species_type'] == 'Fern Ally', 'species_type'] = 'fern ally'

species.loc[species['species_type'] == 'Fern', 'species_type'] = 'fern'

species.loc[species['species_type'] == 'Vine', 'species_type'] = 'vine'

species.loc[species['kingdom'] == 'Bacteria', 'species_type'] = 'bacteria'

species.loc[species['kingdom'] == 'Protozoa', 'species_type'] = 'protozoa'

species.loc[species['kingdom'] == 'Viruses', 'species_type'] = 'virus'

species.loc[species['phylum'] == 'Porifera', 'species_type'] = 'sponge'

species.loc[species['phylum'] == 'Annelida', 'species_type'] = 'round worm'

species.loc[species['phylum'] == 'Echinodermata', 'species_type'] = 'echinoderm'

species.loc[species['phylum'] == 'Bryozoa', 'species_type'] = 'bryozoa'

species.loc[species['phylum'] == 'Rotifera', 'species_type'] = 'rotifer'

species.loc[species['phylum'] == 'Platyhelminthes', 'species_type'] = 'flat worm'

species.loc[species['phylum'] == 'Nemertea', 'species_type'] = 'ribbon worm'

species.loc[species['phylum'] == 'Onychophora', 'species_type'] = 'velvet worm'

species.loc[species['phylum'] == 'Nematoda', 'species_type'] = 'nematode'

species.loc[species['phylum'] == 'Ctenophora', 'species_type'] = 'comb jelly'

species.loc[species['phylum'] == 'Brachiopoda', 'species_type'] = 'brachiopod'

species.loc[species['phylum'] == 'Gastrotricha', 'species_type'] = 'hairy belly'

species.loc[species['phylum'] == 'Tardigrada', 'species_type'] = 'tardigrade'

species.loc[species['phylum'] == 'Sipuncula', 'species_type'] = 'peanut worm'

species.loc[species['phylum'] == 'Nematomorpha', 'species_type'] = 'horsehair worm'

species.loc[species['phylum'] == 'Chaetognatha', 'species_type'] = 'arrow worm'

species.loc[species['phylum'] == 'Phoronida', 'species_type'] = 'horseshoe worm'

species.loc[species['phylum'] == 'Hemichordata', 'species_type'] = 'acorn worm'

species.loc[species['phylum'] == 'Xenacoelomorpha', 'species_type'] = 'xenacoelomorph'

species.loc[species['phylum'] == 'Ciliophora', 'species_type'] = 'ciliate'

# %%

species.loc[species['class'] == 'Mammalia', 'species_type'] = 'mammal'

species.loc[species['class'] == 'Amphibia', 'species_type'] = 'amphibian'

species.loc[species['class'] == 'Reptilia', 'species_type'] = 'reptile'

species.loc[species['class'] == 'Aves', 'species_type'] = 'bird'

species.loc[species['class'] == 'Insecta', 'species_type'] = 'insect'

species.loc[species['class'] == 'Arachnida', 'species_type'] = 'arachnid'

species.loc[species['class'] == 'Diplopoda', 'species_type'] = 'millipede'

species.loc[species['class'] == 'Actinopterygii', 'species_type'] = 'ray fin fish'

species.loc[species['class'] == 'Actinopteri', 'species_type'] = 'ray fin fish'

species.loc[species['class'] == 'Bacillariophyceae', 'species_type'] = 'diatom'

species.loc[species['class'] == 'Phaeophyceae', 'species_type'] = 'brown algae'

species.loc[species['class'] == 'Dinophyceae', 'species_type'] = 'dinoflagellate'

species.loc[species['class'] == 'Prymnesiophyceae', 'species_type'] = 'algae'

species.loc[species['class'] == 'Dictyochophyceae', 'species_type'] = 'heterokont algae'

species.loc[species['class'] == 'Chrysophyceae', 'species_type'] = 'golden brown algae'

species.loc[species['class'] == 'Oligohymenophorea', 'species_type'] = 'ciliate'

species.loc[species['class'] == 'Heterotrichea', 'species_type'] = 'ciliate'

species.loc[species['class'] == 'Hypotrichea', 'species_type'] = 'ciliate'

species.loc[species['class'] == 'Solenogastres', 'species_type'] = 'solenogaster'

species.loc[species['class'] == 'Gastropoda', 'species_type'] = 'slug/snail'

species.loc[species['class'] == 'Bivalvia', 'species_type'] = 'bivalve'

species.loc[species['class'] == 'Malacostraca', 'species_type'] = 'malacostracan'

species.loc[species['class'] == 'Polyplacophora', 'species_type'] = 'chiton'

species.loc[species['class'] == 'Pycnogonida', 'species_type'] = 'sea spider'

species.loc[species['class'] == 'Scyphozoa', 'species_type'] = 'jellyfish'

species.loc[species['class'] == 'Elasmobranchii', 'species_type'] = 'cartilaginous fish'

species.loc[species['class'] == 'Hydrozoa', 'species_type'] = 'hydrozoan'

species.loc[species['class'] == 'Ascidiacea', 'species_type'] = 'ascidian'

species.loc[species['class'] == 'Chilopoda', 'species_type'] = 'centipede'

species.loc[species['class'] == 'Cubozoa', 'species_type'] = 'jellyfish'

species.loc[species['class'] == 'Collembola', 'species_type'] = 'springtail'

species.loc[species['class'] == 'Cephalopoda', 'species_type'] = 'cephalopod'

species.loc[species['class'] == 'Scaphopoda', 'species_type'] = 'tusk shell'

species.loc[species['class'] == 'Ostracoda', 'species_type'] = 'ostracod'

species.loc[species['class'] == 'Holocephali', 'species_type'] = 'cartilaginous fish'

species.loc[species['class'] == 'Staurozoa', 'species_type'] = 'jellyfish'

species.loc[species['class'] == 'Merostomata', 'species_type'] = 'horseshoe crab'

species.loc[species['class'] == 'Myxini', 'species_type'] = 'hagfish'

species.loc[species['class'] == 'Sarcopterygii', 'species_type'] = 'lobe fin fish'

species.loc[species['class'] == 'Appendicularia', 'species_type'] = 'larvacean'

species.loc[species['class'] == 'Anthozoa', 'species_type'] = 'coral/anemone'

species.loc[species['class'] == 'Cephalaspidomorphi', 'species_type'] = 'lamprey'

species.loc[species['class'] == 'Pinopsida', 'species_type'] = 'tree/shrub'

species.loc[species['class'] == 'Bryopsida', 'species_type'] = 'moss'

species.loc[species['class'] == 'Polypodiopsida', 'species_type'] = 'fern'

species.loc[species['class'] == 'Florideophyceae', 'species_type'] = 'red algae'

species.loc[species['class'] == 'Ulvophyceae', 'species_type'] = 'green algae'

species.loc[species['class'] == 'Trebouxiophyceae', 'species_type'] = 'green algae'

species.loc[species['class'] == 'Zygnematophyceae', 'species_type'] = 'green algae'

species.loc[species['class'] == 'Andreaeopsida', 'species_type'] = 'moss'

species.loc[species['class'] == 'Chlorophyceae', 'species_type'] = 'green algae'

species.loc[species['class'] == 'Bangiophyceae', 'species_type'] = 'red algae'

species.loc[species['class'] == 'Cycadopsida', 'species_type'] = 'cycad'

species.loc[species['class'] == 'Charophyceae', 'species_type'] = 'green algae'

species.loc[species['class'] == 'Compsopogonophyceae', 'species_type'] = 'red algae'

species.loc[species['class'] == 'Lycopodiopsida', 'species_type'] = 'lycopod'

species.loc[species['class'] == 'Ginkgoopsida', 'species_type'] = 'tree/shrub'

species.loc[species['class'] == 'Klebsormidiophyceae', 'species_type'] = 'green algae'

species.loc[species['class'] == 'Pyramimonadophyceae', 'species_type'] = 'green algae'

species.loc[species['class'] == 'Sphagnopsida', 'species_type'] = 'moss'

species.loc[species['class'] == 'Haplomitriopsida', 'species_type'] = 'liverwort'

species.loc[species['class'] == 'Jungermanniopsida', 'species_type'] = 'liverwort'

species.loc[species['class'] == 'Anthocerotopsida', 'species_type'] = 'hornwort'

species.loc[species['class'] == 'Marchantiopsida', 'species_type'] = 'liverwort'

species.loc[species['class'] == 'Gnetopsida', 'species_type'] = 'gnetophyte'

# %%
species.loc[species['order'] == 'Poales', 'species_type'] = 'graminoid'

species.loc[species['order'] == 'Pucciniales', 'species_type'] = 'rust fungi'

species.loc[species['order'] == 'Agaricales', 'species_type'] = 'agaric'

species.loc[species['order'] == 'Peltigerales', 'species_type'] = 'lichenized fungi'

species.loc[species['family'] == 'Phallaceae', 'species_type'] = 'stinkhorn'

species.loc[species['family'] == 'Acarosporaceae', 'species_type'] = 'lichenized fungi'

species.loc[species['family'] == 'Arthoniaceae', 'species_type'] = 'lichenized fungi'

species.loc[species['family'] == 'Cladoniaceae', 'species_type'] = 'lichenized fungi'

species.loc[species['family'] == 'Candelariaceae', 'species_type'] = 'lichenized fungi'

species.loc[species['family'] == 'Boletaceae', 'species_type'] = 'bolete'

species.loc[species['family'] == 'Auriculariaceae', 'species_type'] = 'jelly fungi'

species.loc[species['family'] == 'Ganodermataceae', 'species_type'] = 'polypore'

species.loc[species['family'] == 'Geastraceae', 'species_type'] = 'earthstar'

species.loc[species['family'] == 'Physciaceae', 'species_type'] = 'lichenized fungi'

species.loc[species['family'] == 'Parmeliaceae', 'species_type'] = 'lichenized fungi'

species.loc[species['family'] == 'Lecanoraceae', 'species_type'] = 'lichenized fungi'

species.loc[species['family'] == 'Polyporaceae', 'species_type'] = 'polypore'

species.loc[species['family'] == 'Collemataceae', 'species_type'] = 'lichenized fungi'

species.loc[species['family'] == 'Ochrolechiaceae', 'species_type'] = 'lichenized fungi'

species.loc[species['family'] == 'Pannariaceae', 'species_type'] = 'lichenized fungi'

species.loc[species['family'] == 'Peltigeraceae', 'species_type'] = 'lichenized fungi'

species.loc[species['family'] == 'Pertusariaceae', 'species_type'] = 'lichenized fungi'

species.loc[species['family'] == 'Pezizaceae', 'species_type'] = 'cup fungi'

species.loc[species['family'] == 'Lobariaceae', 'species_type'] = 'lichenized fungi'

species.loc[species['family'] == 'Ramalinaceae', 'species_type'] = 'lichenized fungi'


species.loc[species['family'] == 'Orchidaceae', 'species_type'] = 'orchid'

species.loc[species['family'] == 'Salicaceae', 'species_type'] = 'tree/shrub'

species.loc[species['family'] == 'Rousseaceae', 'species_type'] = 'tree/shrub'

species.loc[species['family'] == 'Cactaceae', 'species_type'] = 'cactus'

species.loc[species['family'] == 'Aizoaceae', 'species_type'] = 'succulent'

species.loc[species['family'] == 'Crassulaceae', 'species_type'] = 'succulent'

# %%
species.loc[species['genus'] == 'Acacia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Acer', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Achillea', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Aconitum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Aeonium', 'species_type'] = 'succulent'

species.loc[species['genus'] == 'Agalinis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Iris', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Trifolium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Cirsium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Pedicularis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Aloe', 'species_type'] = 'aloe'

species.loc[species['genus'] == 'Oxytropis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Geranium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Campanula', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Crassula', 'species_type'] = 'succulent'

species.loc[species['genus'] == 'Moraea', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Galium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Drosera', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Quercus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Sedum', 'species_type'] = 'succulent'

species.loc[species['genus'] == 'Potentilla', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Gladiolus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Centaurea', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Eucalyptus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Begonia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Silene', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Ranunculus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Rubus', 'species_type'] = 'bramble'

species.loc[species['genus'] == 'Oxalis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Allium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Acacia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Erica', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Russula', 'species_type'] = 'russula'

species.loc[species['genus'] == 'Aspalathus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Lactarius', 'species_type'] = 'milkcap'

species.loc[species['genus'] == 'Peperomia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Miconia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Primula', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Dianthus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Melaleuca', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Anthurium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Plantago', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Lachenalia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Rhododendron', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Castilleja', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Ruschia', 'species_type'] = 'succulent'

species.loc[species['genus'] == 'Saxifraga', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Grevillea', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Scutellaria', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Alchemilla', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Cliffortia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Impatiens', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Clematis', 'species_type'] = 'vine'

species.loc[species['genus'] == 'Erigeron', 'species_type'] = 'wildflower'

# %%

# species.loc[species['genus'] == 'Utricularia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Agave', 'species_type'] = 'agave'

species.loc[species['genus'] == 'Limonium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Valeriana', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Rosa', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Hermannia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Hibbertia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Linum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Diospyros', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Asclepias', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Berberis', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Delosperma', 'species_type'] = 'succulent'

species.loc[species['genus'] == 'Palicourea', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Gentiana', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Psychotria', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Thymus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Babiana', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Prunus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Erythranthe', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Convolvulus', 'species_type'] = 'vine'

species.loc[species['genus'] == 'Muraltia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Verbesina', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Draba', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Rumex', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Delphinium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Lupinus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Stylidium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Eryngium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Lampranthus', 'species_type'] = 'succulent'

species.loc[species['genus'] == 'Ornithogalum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Oenothera', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Cardamine', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Dioscorea', 'species_type'] = 'vine'

species.loc[species['genus'] == 'Heliconia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Boronia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Echeveria', 'species_type'] = 'succulent'

species.loc[species['genus'] == 'Taraxacum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Colchicum', 'species_type'] = 'wildflower'

# %%

# species.loc[species['genus'] == 'Wahlenbergia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Linaria', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Cerastium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Penstemon', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Romulea', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Phacelia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Hakea', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Protea', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Epilobium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Gentianella', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Geissorhiza', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Matelea', 'species_type'] = 'vine'

species.loc[species['genus'] == 'Lotus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Crataegus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Crocus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Orobanche', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Saussurea', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Vachellia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Albuca', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Persicaria', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Mesembryanthemum', 'species_type'] = 'succulent'

species.loc[species['genus'] == 'Drosanthemum', 'species_type'] = 'succulent'

species.loc[species['genus'] == 'Ixia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Pultenaea', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Papaver', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Psoralea', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Berkheya', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Ramaria', 'species_type'] = 'coral fungi'

species.loc[species['genus'] == 'Searsia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Eremophila', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Corydalis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Sisyrinchium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Syzygium', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Crepis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Combretum', 'species_type'] = 'tree/shrub'

# %%

finalj = species.to_json(orient='records', force_ascii=False)
repr(finalj)


# %%

file = open("species_prep_2.json", "w")
file.write(finalj)
file.close

# %%


species = pd.read_json('/home/muskrat/Desktop/species_prep_2.json')

# %%

# %%

fungi = species[(species['kingdom'] == 'Fungi')]

chromista = species[(species['kingdom'] == 'Chromista')]

bacteria = species[(species['kingdom'] == 'Bacteria')]

protozoa = species[(species['kingdom'] == 'Protozoa')]

virus = species[(species['kingdom'] == 'Viruses')]

animal = species[(species['kingdom'] == 'Animalia')]

plant = species[(species['kingdom'] == 'Plantae')]


non_plant = plant[(plant['species_type'].isna())]

non_animal = animal[(animal['species_type'].isna())]

non_fungi = fungi[(fungi['species_type'].isna())]

neme = species[(species['species_type'].isna())]

# %%

tester = neme['genus'].value_counts()

# head = animal.head(n=1000)
# none2 = animal[(animal['family'].isna())]

# %%

species.loc[species['class'] == 'Lecanoromycetes', 'species_type'] = 'lichenized fungi'

species.loc[species['class'] == 'Ustilaginomycetes', 'species_type'] = 'smut fungi'

species.loc[species['class'] == 'Dacrymycetes', 'species_type'] = 'jelly fungi'

species.loc[species['class'] == 'Geoglossomycetes', 'species_type'] = 'earth tongue'

species.loc[species['class'] == 'Saccharomycetes', 'species_type'] = 'budding yeast'

species.loc[species['class'] == 'Neolectomycetes', 'species_type'] = 'earth tongue like'



species.loc[species['order'] == 'Verrucariales', 'species_type'] = 'lichenized fungi'

species.loc[species['order'] == 'Arthoniales', 'species_type'] = 'lichenized fungi'

species.loc[species['order'] == 'Eurotiales', 'species_type'] = 'green and blue mold'

species.loc[species['order'] == 'Lichinales', 'species_type'] = 'lichenized fungi'

species.loc[species['order'] == 'Trypetheliales', 'species_type'] = 'lichenized fungi'

species.loc[species['order'] == 'Mucorales', 'species_type'] = 'pin mold'

species.loc[species['order'] == 'Entylomatales', 'species_type'] = 'smut fungi'

species.loc[species['order'] == 'Lepidostromatales', 'species_type'] = 'lichenized club fungi'





species.loc[species['order'] == 'Diplostraca', 'species_type'] = 'water flea'

species.loc[species['order'] == 'Anostraca', 'species_type'] = 'fairy shrimp'

species.loc[species['order'] == 'Cyclopoida', 'species_type'] = 'plankton'

species.loc[species['order'] == 'Cyclopoida', 'species_type'] = 'barnacle'

species.loc[species['order'] == 'Salpida', 'species_type'] = 'salp'

species.loc[species['order'] == 'Pedunculata', 'species_type'] = 'barnacle'

species.loc[species['order'] == 'Notostraca', 'species_type'] = 'tadpole shrimp'

species.loc[species['order'] == 'Diplura', 'species_type'] = 'dipluran'

species.loc[species['order'] == 'Calanoida', 'species_type'] = 'plankton'

species.loc[species['order'] == 'Kentrogonida', 'species_type'] = 'barnacle'

species.loc[species['order'] == 'Arguloida', 'species_type'] = 'fish lice'

# %%

species.loc[species['family'] == 'Cantharellaceae', 'species_type'] = 'gill like'

species.loc[species['family'] == 'Phanerochaetaceae', 'species_type'] = 'crust fungi'

species.loc[species['family'] == 'Stereaceae', 'species_type'] = 'crust fungi'

species.loc[species['family'] == 'Sarcoscyphaceae', 'species_type'] = 'cup fungi'

species.loc[species['family'] == 'Thelephoraceae', 'species_type'] = 'leathery earthfan'

species.loc[species['family'] == 'Coniocybaceae', 'species_type'] = 'lichenized fungi'

species.loc[species['family'] == 'Dermateaceae', 'species_type'] = 'cup fungi'

species.loc[species['family'] == 'Atheliaceae', 'species_type'] = 'crust fungi'

species.loc[species['family'] == 'Pyrenulaceae', 'species_type'] = 'lichenized fungi'



species.loc[species['family'] == 'Balanidae', 'species_type'] = 'barnacle'

species.loc[species['family'] == 'Daphniidae', 'species_type'] = 'water flea'

species.loc[species['family'] == 'Chthamalidae', 'species_type'] = 'barnacle'

species.loc[species['family'] == 'Chydoridae', 'species_type'] = 'water flea'

species.loc[species['family'] == 'Lepadidae', 'species_type'] = 'barnacle'

species.loc[species['family'] == 'Tetraclitidae', 'species_type'] = 'barnacle'


# %%
species.loc[species['genus'] == 'Suillus', 'species_type'] = 'slippery jack'

species.loc[species['genus'] == 'Helvella', 'species_type'] = 'elfin saddle'

species.loc[species['genus'] == 'Morchella', 'species_type'] = 'morel'

species.loc[species['genus'] == 'Hydnellum', 'species_type'] = 'tooth fungi'

species.loc[species['genus'] == 'Lactifluus', 'species_type'] = 'milk cap'

species.loc[species['genus'] == 'Phellinus', 'species_type'] = 'cork fungi'

species.loc[species['genus'] == 'Scutellinia', 'species_type'] = 'cup fungi'

species.loc[species['genus'] == 'Peniophora', 'species_type'] = 'crust fungi'

species.loc[species['genus'] == 'Scleroderma', 'species_type'] = 'earth ball'

species.loc[species['genus'] == 'Steccherinum', 'species_type'] = 'toothed crust fungi'

species.loc[species['genus'] == 'Lentinellus', 'species_type'] = 'lamellate agaric'

species.loc[species['genus'] == 'Chroogomphus', 'species_type'] = 'pine spike'

species.loc[species['genus'] == 'Laetiporus', 'species_type'] = 'polypore'

species.loc[species['genus'] == 'Clavariadelphus', 'species_type'] = 'club fungi'

species.loc[species['genus'] == 'Phellodon', 'species_type'] = 'tooth fungi'

species.loc[species['genus'] == 'Trichaptum', 'species_type'] = 'polypore'

species.loc[species['genus'] == 'Artomyces', 'species_type'] = 'club fungi'

species.loc[species['genus'] == 'Phaeoclavulina', 'species_type'] = 'club fungi'

species.loc[species['genus'] == 'Calostoma', 'species_type'] = 'gasteroid'

species.loc[species['genus'] == 'Austropaxillus', 'species_type'] = 'gill'

species.loc[species['genus'] == 'Gomphidius', 'species_type'] = 'spike cap'

species.loc[species['genus'] == 'Fuscoporia', 'species_type'] = 'polypore'

species.loc[species['genus'] == 'Diatrype', 'species_type'] = 'crust fungi'

species.loc[species['genus'] == 'Hericium', 'species_type'] = 'hericium'

species.loc[species['genus'] == 'Chaenothecopsis', 'species_type'] = 'lichenized fungi'

species.loc[species['genus'] == 'Claviceps', 'species_type'] = 'ergot'

species.loc[species['genus'] == 'Xylodon', 'species_type'] = 'crust fungi'

species.loc[species['genus'] == 'Rhytisma', 'species_type'] = 'tar spot'

species.loc[species['genus'] == 'Annulohypoxylon', 'species_type'] = 'cramp ball'

species.loc[species['genus'] == 'Sarcodon', 'species_type'] = 'tooth fungi'

species.loc[species['genus'] == 'Rhizopogon', 'species_type'] = 'false truffle'

species.loc[species['genus'] == 'Fomitopsis', 'species_type'] = 'polypore'

species.loc[species['genus'] == 'Astraeus', 'species_type'] = 'earth star'

species.loc[species['genus'] == 'Strigula', 'species_type'] = 'lichenized fungi'
# %%

species.loc[species['genus'] == 'Drimia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Hydrocotyle', 'species_type'] = 'aquatic'

species.loc[species['genus'] == 'Lepidium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Agathosma', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Zephyranthes', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Smilax', 'species_type'] = 'smilax'

species.loc[species['genus'] == 'Verbascum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Coprosma', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Genista', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Viburnum', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Celmisia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Prostanthera', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Euphrasia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Scrophularia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Brickellia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Terminalia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Selago', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Lycium', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Myosotis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Leucopogon', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Stellaria', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Fritillaria', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Erysimum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Ceropegia', 'species_type'] = 'vine'

species.loc[species['genus'] == 'Bidens', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Leptospermum', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Eugenia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Erodium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Pinguicula', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Antimima', 'species_type'] = 'succulent'

species.loc[species['genus'] == 'Erythrina', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Gagea', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Hieracium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Solidago', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Lilium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Clinopodium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Watsonia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Leucadendron', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Cuscuta', 'species_type'] = 'vine'

species.loc[species['genus'] == 'Micranthes', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Jamesbrittenia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Ribes', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Aquilegia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Daviesia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Gaultheria', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Kniphofia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Chenopodium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Thalictrum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Cynanchum', 'species_type'] = 'vine'

species.loc[species['genus'] == 'Cissus', 'species_type'] = 'vine'

species.loc[species['genus'] == 'Cotoneaster', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Buddleja', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Cordia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Senegalia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Arenaria', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Bauhinia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Azorella', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Symplocos', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Polygonum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Onobrychis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Costus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Ardisia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Tragopogon', 'species_type'] = 'wildflower'

# %%

species.loc[species['genus'] == 'Alpinia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Ludwigia', 'species_type'] = 'aquatic'

species.loc[species['genus'] == 'Banksia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Pittosporum', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Paronychia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Androsace', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Hypoxis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Vincetoxicum', 'species_type'] = 'vine'

species.loc[species['genus'] == 'Lomatium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Boechera', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Vaccinium', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Tabernaemontana', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Hesperantha', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Lippia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Echinops', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Commelina', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Mandevilla', 'species_type'] = 'vine'

species.loc[species['genus'] == 'Bursera', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Angelica', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Diplacus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Carduus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Albizia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Bossiaea', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Aristea', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Fuchsia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Glandularia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Sonchus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Arctotis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Seseli', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Streptocarpus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Arisaema', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Alstroemeria', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Dracaena', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Nepeta', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Commiphora', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Aciphylla', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Cryptocarya', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Pauridia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Reseda', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Erythroxylum', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Allocasuarina', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Crinum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Vitex', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Lomandra', 'species_type'] = 'graminoid'

species.loc[species['genus'] == 'Gymnosporia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Portulaca', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Dodonaea', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Zaluzianskya', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Espeletia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Physaria', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Arabis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Bulbine', 'species_type'] = 'succulent'

species.loc[species['genus'] == 'Centella', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Corymbia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Goeppertia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Gypsophila', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Prosopis', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Potamogeton', 'species_type'] = 'aquatic'

species.loc[species['genus'] == 'Pulsatilla', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Tropaeolum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Pandanus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Grindelia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Ferula', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Symphyotrichum', 'species_type'] = 'wildflower'

# %%


finalj = species.to_json(orient='records', force_ascii=False)
repr(finalj)


# %%

file = open("species_prep_3.json", "w")
file.write(finalj)
file.close

# %%

species.loc[species['family'] == 'Nymphaeaceae', 'species_type'] = 'aquatic'

species.loc[species['family'] == 'Potamogetonaceae', 'species_type'] = 'aquatic'




species.loc[species['genus'] == 'Posidonia', 'species_type'] = 'aquatic'

species.loc[species['genus'] == 'Acorus', 'species_type'] = 'graminoid'

species.loc[species['genus'] == 'Zizania', 'species_type'] = 'aquatic'

species.loc[species['genus'] == 'Sagittaria', 'species_type'] = 'aquatic'

species.loc[species['genus'] == 'Pistia', 'species_type'] = 'aquatic'

species.loc[species['genus'] == 'Lemna', 'species_type'] = 'aquatic'

species.loc[species['genus'] == 'Wolffia', 'species_type'] = 'aquatic'

species.loc[species['genus'] == 'Hydrocharis', 'species_type'] = 'aquatic'

species.loc[species['genus'] == 'Calochortus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Cotula', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Scilla', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Sterculia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Pseudognaphalium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Cymopterus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Anthemis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Pomaderris', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Hypochaeris', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Elaeocarpus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Lachnaea', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Ixora', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Cestrum', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Cyrtanthus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Epacris', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Ligularia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Nepenthes', 'species_type'] = 'vine'

species.loc[species['genus'] == 'Daphne', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Navarretia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Tanacetum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Dracocephalum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Erepsia', 'species_type'] = 'succulent'

species.loc[species['genus'] == 'Anchusa', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Garcinia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Ajuga', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Spiraea', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Rhamnus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Ocotea', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Phoradendron', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Narcissus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Phlox', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Haworthia', 'species_type'] = 'succulent'

species.loc[species['genus'] == 'Annona', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Diascia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Fumaria', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Ayenia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Geum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Roldana', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Trillium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Vitis', 'species_type'] = 'vine'

species.loc[species['genus'] == 'Litsea', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Salicornia', 'species_type'] = 'succulent'

species.loc[species['genus'] == 'Amyema', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Chorizanthe', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Xanthorrhoea', 'species_type'] = 'grasstree'

species.loc[species['genus'] == 'Nymphaea', 'species_type'] = 'aquatic'

species.loc[species['genus'] == 'Petrophile', 'species_type'] = 'tree/shrub'

# %%

species.loc[species['genus'] == 'Glechoma', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Cichorium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Ficaria', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Lamium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Daucus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Hedera', 'species_type'] = 'vine'

species.loc[species['genus'] == 'Tussilago', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Chelidonium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Ailanthus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Lythrum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Bellis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Capsella', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Leucanthemum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Lamium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Reynoutria', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Dipsacus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Sorbus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Aegopodium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Ricinus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Saponaria', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Filipendula', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Anthriscus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Frangula', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Cytisus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Corylus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Betula', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Calyptocarpus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Barbarea', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Elaeagnus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Rabelera', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Ligustrum', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Humulus', 'species_type'] = 'vine'

species.loc[species['genus'] == 'Thlaspi', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Fagus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Nandina', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Sherardia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Calluna', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Melia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Centranthus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Calystegia', 'species_type'] = 'vine'

species.loc[species['genus'] == 'Conium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Alnus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Raphanus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Aesculus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Galanthus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Morus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Paulownia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Hepatica', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Origanum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Silybum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Arctium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Ulex', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Arctium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Anemonastrum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Dipterostemon', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Heracleum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Monotropa', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Datura', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Youngia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Broussonetia', 'species_type'] = 'tree/shrub'
# %%



species.loc[species['genus'] == 'Fraxinus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Maianthemum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Schinus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Lycopus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Torilis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Adelinia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Arum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Borago', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Viscum', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Asarum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Pentaglottis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Zantedeschia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Tilia', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Paris', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Catharanthus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Pyrus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Symphytum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Agrimonia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Cornus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Syringa', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Cocos', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Pontederia', 'species_type'] = 'aquatic'

species.loc[species['genus'] == 'Melampyrum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Pulmonaria', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Lobularia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Carpinus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Hippophae', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Carpobrotus', 'species_type'] = 'succulent'

species.loc[species['genus'] == 'Nerium', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Anemone', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Alisma', 'species_type'] = 'aquatic'

species.loc[species['genus'] == 'Hemerocallis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Malus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Bunias', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Spartium', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Cosmos', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Castanea', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Pterospora', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Leucaena', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Carica', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Picris', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Chrysosplenium', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Rhinanthus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Sapindus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Fragaria', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Bistorta', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Modiola', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Dermatophyllum', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Ruscus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Plectocephalus', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Caragana', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Ulmus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Houstonia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Eranthis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Cota', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Pistacia', 'species_type'] = 'tree/shrub'

# %%

species.loc[species['genus'] == 'Macaranga', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Hirschfeldia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Crithmum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Antigonon', 'species_type'] = 'vine'

species.loc[species['genus'] == 'Juglans', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Stylophorum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Onopordum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Rhodotypos', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Olea', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Muscari', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Cercocarpus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Mentha', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Psidium', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Caulophyllum', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Calendula', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Melissa', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Burchardia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Arbutus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Hyacinthoides', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Colocasia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Pyrola', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Delairea', 'species_type'] = 'vine'

species.loc[species['genus'] == 'Richardia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Cistus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Cyclamen', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Leopoldia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Arabidopsis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Punica', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Bellardia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Myriophyllum', 'species_type'] = 'aquatic'

species.loc[species['genus'] == 'Cynara', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Parietaria', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Descurainia', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Galeopsis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Buglossoides', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Cannabis', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Cotinus', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Lathraea', 'species_type'] = 'wildflower'

species.loc[species['genus'] == 'Mangifera', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Casuarina', 'species_type'] = 'tree/shrub'

species.loc[species['genus'] == 'Amaryllis', 'species_type'] = 'wildflower'

# %%





species.loc[species['scientific_name'] == 'Alliaria petiolata', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Artemisia vulgaris', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Solanum dulcamara', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Veronica persica', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Melilotus albus', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Vinca minor', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Lonicera japonica', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Lysimachia arvensis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Veronica chamaedrys', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Echium vulgare', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Medicago lupulina', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Securigera varia', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Digitalis purpurea', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Vicia cracca', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Hypericum perforatum', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Lonicera maackii', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Senecio vulgaris', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Hesperis matronalis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Celastrus orbiculatus', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Anemonoides nemorosa', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Convallaria majalis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Lantana camara', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Vinca major', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Ambrosia trifida', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Melilotus officinalis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Ilex aquifolium', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Vicia sativa', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Lactuca serriola', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Solanum elaeagnifolium', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Berteroa incana', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Foeniculum vulgare', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Jacobaea vulgaris', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Tripleurospermum inodorum', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Ampelopsis glandulosa', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Solanum carolinense', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Nicotiana glauca', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Cymbalaria muralis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Lunaria annua', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Euonymus alatus', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Lysimachia nummularia', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Medicago sativa', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Leonurus cardiaca', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Vicia villosa', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Knautia arvensis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Lapsana communis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Sambucus cerulea', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Sambucus canadensis', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Lysimachia vulgaris', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Lathyrus latifolius', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Pastinaca sativa', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Veronica serpyllifolia', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Marrubium vulgare', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Vicia sepium', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Triadica sebifera', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Malva sylvestris', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Solanum nigrum', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Conopholis americana', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Lathyrus pratensis', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Veronica arvensis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Euonymus fortunei', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Mycelis muralis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Euphorbia peplus', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Galinsoga quadriradiata', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Euphorbia cyparissias', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Artemisia absinthium', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Euonymus europaeus', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Helminthotheca echioides', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Eupatorium cannabinum', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Cakile maritima', 'species_type'] = 'succulent'

species.loc[species['scientific_name'] == 'Ficus carica', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Stachys sylvatica', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Anemonoides quinquefolia', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Ipomoea purpurea', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Anemonoides ranunculoides', 'species_type'] = 'wildflower'



# %%


finalj = species.to_json(orient='records', force_ascii=False)
repr(finalj)


# %%

file = open("species_prep_5.json", "w")
file.write(finalj)
file.close

# %%

species = pd.read_json('/home/muskrat/Desktop/species_prep_4.json')

tester = pd.read_json('/home/muskrat/Desktop/tester2.json')

# %%

species.loc[species['scientific_name'] == 'Lysimachia latifolia', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Cocculus carolinus', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Lathyrus vernus', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Euphorbia virgata', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Viola arvensis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Sphagneticola trilobata', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Leonurus quinquelobatus', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Tridax procumbens', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Acmispon glaber', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Lonicera xylosteum', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Perilla frutescens', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Viola odorata', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Hibiscus syriacus', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Ruellia simplex', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Mirabilis jalapa', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Asparagus officinalis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Medicago falcata', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Solanum rostratum', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Heracleum sosnowskyi', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Parthenium hysterophorus', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Abutilon theophrasti', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Marah macrocarpa', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Veronica longifolia', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Cynoglossum officinale', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Euphorbia helioscopia', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Butomus umbellatus', 'species_type'] = 'aquatic'

species.loc[species['scientific_name'] == 'Salvia pratensis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Tribulus terrestris', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Euphorbia prostrata', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Malva moschata', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Mercurialis perennis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Betonica officinalis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Nasturtium officinale', 'species_type'] = 'aquatic'

species.loc[species['scientific_name'] == 'Peritoma arborea', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Euphorbia hirta', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Plumbago auriculata', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Sanguisorba officinalis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Melilotus indicus', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Ballota nigra', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Lysimachia punctata', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Veronica hederifolia', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Malva neglecta', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Medicago polymorpha', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Scorzoneroides autumnalis', 'species_type'] = 'wildflower'
# %%

species.loc[species['scientific_name'] == 'Viola tricolor', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Croton setiger', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Glebionis coronaria', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Succisa pratensis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Ipomoea indica', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Tradescantia fluminensis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Sisymbrium loeselii', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Osteospermum moniliferum', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Jasione montana', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Polygonatum multiflorum', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Solanum lycopersicum', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Anthyllis vulneraria', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Euonymus verrucosus', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Momordica charantia', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Rapistrum rugosum', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Calandrinia menziesii', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Polygonatum odoratum', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Diodia virginiana', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Circaea canadensis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Passiflora foetida', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Thunbergia alata', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Euphorbia marginata', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Lespedeza cuneata', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Sisymbrium officinale', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Lonicera tatarica', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Trollius europaeus', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Solanum mauritianum', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Centaurium erythraea', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Senecio inaequidens', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Ipomoea cairica', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Toxicoscordion fremontii', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Pachysandra terminalis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Pilea microphylla', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Phlomoides tuberosa', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Mimosa pudica', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Verbena brasiliensis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Oncosiphon pilulifer', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Cordyline australis', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Urena lobata', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Urtica urens', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Pimpinella saxifraga', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Pentanema britannicum', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Sanguisorba minor', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Sinapis arvensis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Lathyrus sylvestris', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Lathyrus tuberosus', 'species_type'] = 'vine'

# %%

species.loc[species['scientific_name'] == 'Cnidoscolus stimulosus', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Lonicera periclymenum', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Mazus pumilus', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Taraxia ovata', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Petasites hybridus', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Echium plantagineum', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Euphorbia lathyris', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Melicytus ramiflorus', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Hylotelephium telephium', 'species_type'] = 'succulent'

species.loc[species['scientific_name'] == 'Dichondra carolinensis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Malva parviflora', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Marah fabacea', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Viscaria vulgaris', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Passiflora caerulea', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Clintonia uniflora', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Eriobotrya japonica', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Arctotheca calendula', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Alcea rosea', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Cardiospermum halicacabum', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Veronica filiformis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Ageratina adenophora', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Consolida regalis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Umbilicus rupestris', 'species_type'] = 'succulent'

species.loc[species['scientific_name'] == 'Armoracia rusticana', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Solanum dimidiatum', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Mitella diphylla', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Panax trifolius', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Scaevola taccada', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Pueraria montana', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Lindheimera texana', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Inula helenium', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Deinandra fasciculata', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Marah oregana', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Polanisia dodecandra', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Hibiscus trionum', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Pulicaria dysenterica', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Euphorbia hypericifolia', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Odontites vulgaris', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Galinsoga parviflora', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Malva thuringiaca', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Medicago arabica', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Hyoscyamus niger', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Tradescantia pallida', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Euphorbia polycarpa', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Verbena bonariensis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Viola riviniana', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Brassica tournefortii', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Teucrium scorodonia', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Helichrysum arenarium', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Calotropis procera', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Ipomoea hederacea', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Sambucus ebulus', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Echium candicans', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Fallopia convolvulus', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Salvia rosmarinus', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Adonis vernalis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Acanthus mollis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Camissoniopsis cheiranthifolia', 'species_type'] = 'wildflower'
# %%

species.loc[species['scientific_name'] == 'Chorispora tenella', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Hardenbergia violacea', 'species_type'] = 'vine'

species.loc[species['scientific_name'] == 'Anticlea elegans', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Sisymbrium irio', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Chamaecytisus ruthenicus', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Leonotis nepetifolia', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Veronica spicata', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Malvastrum coromandelianum', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Polygala lutea', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Euphorbia serpens', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Euphorbia bicolor', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Verbena officinalis', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Veronica beccabunga', 'species_type'] = 'succulent'

species.loc[species['scientific_name'] == 'Condea emoryi', 'species_type'] = 'tree/shrub'

species.loc[species['scientific_name'] == 'Euphorbia nutans', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Helleborus foetidus', 'species_type'] = 'wildflower'

species.loc[species['scientific_name'] == 'Passiflora suberosa', 'species_type'] = 'vine'

# %%

finalj = species.to_json(orient='records', force_ascii=False)
repr(finalj)


# %%

file = open("species_prep_6.json", "w")
file.write(finalj)
file.close

#%%

species = pd.read_json('/media/muskrat/060A9B8E0A9B78FF/eco_data/main_eco_data/final/species_v1/species_v1.json')

#%%

# species.drop('same', 1, inplace=True)

# # %%

# finalj = species.to_json(orient='records', force_ascii=False)


# # %%

# file = open("species_v1.json", "w")
# file.write(finalj)
# file.close

#%%

common = species.common_name.unique()
types = species.species_type.unique()
kingdom = species.kingdom.unique()

#%%

species['kingdom'].value_counts()

#%%

species['species_type'].replace(['gill', 'russula', 'gill like', 'milkcap', 'milk cap', 'spike cap', 'lamellate agaric', 'agaric', 'pine spike'], 'gill_fungi', inplace=True)

species['species_type'].replace(['earthstar', 'earth star', 'earth ball', 'false truffle', 'stinkhorn', 'gasteroid'], 'gasteroid_fungi', inplace=True)

species['species_type'].replace(['club fungi', 'cup fungi', 'polypore', 'cork fungi', 'tooth fungi', 'bolete', 'jelly fungi', 'leathery earthfan', 'earth tongue', 'earth tongue like', 'coral fungi', 'elfin saddle', 'hericium', 'cramp ball', 'morel', 'slippery jack', 'crust fungi', 'toothed crust fungi'], 'non_gilled_fungi', inplace=True)

species['species_type'].replace(['lichenized fungi', 'lichenized club fungi', 'rust fungi', 'green and blue mold', 'pin mold', 'smut fungi', 'tar spot', 'budding yeast', 'ergot'], 'other_fungi', inplace=True)

#%%

species['species_type'].replace(['tree/shrub', 'gnetophyte', 'cycad', 'bramble', 'grasstree'], 'tree_shrub', inplace=True)

species['species_type'].replace(['smilax',], 'vine', inplace=True)

species['species_type'].replace(['aquatic', 'cactus', 'succulent', 'aloe', 'agave'], 'water_master', inplace=True)

species['species_type'].replace(['orchid'], 'wildflower', inplace=True)

species['species_type'].replace(['moss', 'fern', 'liverwort', 'lycopod', 'hornwort', 'fern ally'], 'other_plants', inplace=True)
#%%
species['species_type'].replace(['brown algae', 'algae', 'heterokont algae', 'golden brown algae', 'red algae', 'green algae', 'diatom', 'dinoflagellate'], 'algae', inplace=True)

#%%

species['species_type'].replace(['centipede', 'fish lice', 'fairy shrimp', 'dipluran', 'springtail', 'insect', 'arachnid', 'millipede', 'malacostracan', 'barnacle', 'water flea', 'plankton', 'sea spider', 'tadpole shrimp', 'ostracod', 'horseshoe crab'], 'arthropod', inplace=True)

species['species_type'].replace(['cartilaginous fish', 'ray fin fish', 'hagfish', 'lamprey', 'lobe fin fish'], 'fish', inplace=True)

species['species_type'].replace(['jellyfish', 'coral/anemone', 'hydrozoan'], 'cnidaria', inplace=True)

species['species_type'].replace(['cephalopod', 'slug/snail', 'chiton', 'bivalve', 'tusk shell', 'solenogaster'], 'mollusk', inplace=True)

species['species_type'].replace(['ascidian', 'larvacean', 'salp'], 'tunicate', inplace=True)

species['species_type'].replace(['round worm', 'velvet worm', 'flat worm', 'echinoderm', 'ribbon worm', 'nematode', 'tardigrade', 'arrow worm', 'rotifer', 'hairy belly', 'horsehair worm', 'acorn worm', 'peanut worm', 'xenacoelomorph'], 'worm', inplace=True)

species['species_type'].replace(['sponge', 'bryozoa', 'brachiopod', 'comb jelly', 'horseshoe worm', 'tunicate'], 'other_animals', inplace=True)

#%%

species.loc[species['kingdom'] == 'Archaea', 'species_type'] = 'archaea'

species.loc[species['phylum'] == 'Arthropoda', 'species_type'] = 'arthropod'

species.loc[species['phylum'] == 'Entoprocta', 'species_type'] = 'worm'

species.loc[species['phylum'] == 'Priapulida', 'species_type'] = 'worm'

species.loc[species['phylum'] == 'Kinorhyncha', 'species_type'] = 'worm'

species.loc[species['phylum'] == 'Acanthocephala', 'species_type'] = 'worm'

species.loc[species['phylum'] == 'Mollusca', 'species_type'] = 'mollusk'

species.loc[species['phylum'] == 'Cnidaria', 'species_type'] = 'cnidaria'

species.loc[species['class'] == 'Thaliacea', 'species_type'] = 'other_animals'

species.loc[species['class'] == 'Leptocardii', 'species_type'] = 'fish'

species.loc[species['order'] == 'Scorpaeniformes', 'species_type'] = 'fish'

species.loc[species['genus'] == 'Palaeomystella', 'species_type'] = 'arthropod'

species.loc[species['genus'] == 'Phyllodonta', 'species_type'] = 'arthropod'

species.loc[species['genus'] == 'Archaeoaenetus', 'species_type'] = 'arthropod'

species.loc[species['genus'] == 'Hammatoderus', 'species_type'] = 'arthropod'

species.loc[species['genus'] == 'Mnesarchella', 'species_type'] = 'arthropod'

species.loc[species['genus'] == 'Vaejovis', 'species_type'] = 'arthropod'

species.loc[species['genus'] == 'Chlorocalis', 'species_type'] = 'arthropod'

species.loc[species['scientific_name'] == 'Garateia ximenae', 'species_type'] = 'arthropod'

species.loc[species['scientific_name'] == 'Kuschelorhynchus macadamiae', 'species_type'] = 'arthropod'

species.loc[species['scientific_name'] == 'Baculomia siamensis', 'species_type'] = 'arthropod'

species.loc[species['scientific_name'] == 'Xenocerogria ruficollis', 'species_type'] = 'arthropod'

#%%

species.loc[(species['kingdom'] == 'Fungi') & (species['species_type'].isna()), 'species_type'] = 'uncategorized_fungi'

species.loc[(species['kingdom'] == 'Plantae') & (species['species_type'].isna()), 'species_type'] = 'uncategorized_plants'

# %%

finalj = species.to_json(orient='records', force_ascii=False)



# %%

file = open("species_v1.1.json", "w")
file.write(finalj)
file.close

















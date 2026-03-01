export interface CityVoice {
  satisfactions: string[];
  dissatisfactions: string[];
}

export const CITIZEN_VOICE: Record<string, CityVoice> = {
  "75056": {
    satisfactions: ["Transports en commun", "Offre culturelle", "Espaces verts"],
    dissatisfactions: ["Propreté des rues", "Coût du logement", "Nuisances sonores"],
  },
  "13055": {
    satisfactions: ["Cadre de vie et climat", "Plages et littoral", "Gastronomie locale"],
    dissatisfactions: ["Insécurité", "Transports en commun", "Logement insalubre"],
  },
  "69123": {
    satisfactions: ["Pistes cyclables", "Gastronomie", "Universités et recherche"],
    dissatisfactions: ["ZFE et restrictions auto", "Stationnement", "Vie nocturne et bruit"],
  },
  "31555": {
    satisfactions: ["Aérospatiale et emploi", "Cadre de vie", "Universités"],
    dissatisfactions: ["Transports en commun", "Stationnement", "Étalement urbain"],
  },
  "06088": {
    satisfactions: ["Climat et cadre de vie", "Promenade des Anglais", "Sécurité"],
    dissatisfactions: ["Coût de la vie", "Stationnement", "Tourisme de masse"],
  },
  "44109": {
    satisfactions: ["Transports en commun", "Cadre de vie", "Dynamisme économique"],
    dissatisfactions: ["Insécurité", "Propreté des rues", "Risque inondations"],
  },
  "34172": {
    satisfactions: ["Tramway", "Universités", "Climat"],
    dissatisfactions: ["Insécurité", "Circulation", "Urbanisation rapide"],
  },
  "67482": {
    satisfactions: ["Tramway et transports", "Centre historique", "Dimension européenne"],
    dissatisfactions: ["Stationnement", "Propreté", "Coût du logement"],
  },
  "33063": {
    satisfactions: ["Cadre de vie", "Tramway", "Gastronomie et vin"],
    dissatisfactions: ["Coût du logement", "Tourisme de masse", "Circulation"],
  },
  "59350": {
    satisfactions: ["Offre culturelle", "Transports métropolitains", "Universités"],
    dissatisfactions: ["Propreté des rues", "Insécurité", "Climat et grisaille"],
  },
};

export const DEFAULT_VOICE: CityVoice = {
  satisfactions: ["Cadre de vie", "Offre de services", "Espaces verts"],
  dissatisfactions: ["Coût de la vie", "Stationnement", "Propreté"],
};

export function getCityVoice(commune: string): CityVoice {
  return CITIZEN_VOICE[commune] || DEFAULT_VOICE;
}

import dns.resolver
import itertools
import tldextract

def get_subdomain_combinations(base_domain):
    # Извлекаем суффикс домена
    ext = tldextract.extract(base_domain)
    # Получаем все возможные комбинации поддоменов
    subdomains = ['www', 'mail', 'ftp', 'api', 'shop']  # можно добавить свои поддомены
    combinations = ['.'.join(x) + '.' + ext.domain + '.' + ext.suffix for x in itertools.permutations(subdomains, 1)]
    combinations += ['www.' + ext.domain + '.' + ext.suffix]  # добавляем основной домен
    return combinations

def resolve_domains(domains):
    resolved_ips = {}
    for domain in domains:
        try:
            answers = dns.resolver.resolve(domain, 'A')
            resolved_ips[domain] = [answer.to_text() for answer in answers]
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.Timeout):
            resolved_ips[domain] = None
    return resolved_ips

if __name__ == "__main__":
    base_domain = "pornhub"  # Замените на нужный домен
    subdomains = get_subdomain_combinations(base_domain)
    resolved_ips = resolve_domains(subdomains)

    for domain, ips in resolved_ips.items():
        print(f"Домен: {domain}, IP-адреса: {ips}")
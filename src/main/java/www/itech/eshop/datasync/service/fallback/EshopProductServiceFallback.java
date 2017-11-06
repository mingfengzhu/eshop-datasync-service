package www.itech.eshop.datasync.service.fallback;

import org.springframework.stereotype.Component;

import www.itech.eshop.datasync.service.EshopProductService;

@Component
public class EshopProductServiceFallback implements EshopProductService {

	public String findBrandById(Long id) {
		
		return null;
	}

	public String findBrandByIds(String ids) {
		
		return null;
	}

	public String findCategoryById(Long id) {
		
		return null;
	}

	public String findProductIntroById(Long id) {
		
		return null;
	}

	public String findProductPropertyById(Long id) {
		
		return null;
	}

	public String findProductById(Long id) {
		
		return null;
	}

	public String findProductSpecificationById(Long id) {
		
		return null;
	}

}
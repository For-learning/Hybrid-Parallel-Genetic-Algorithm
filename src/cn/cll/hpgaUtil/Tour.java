package cn.cll.hpgaUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * ÿ��Tour������Ⱥ��һ�� ����
 */
public class Tour {

	// �������г��е�·��
	private List<City> tour = new ArrayList<City>();
	// Cache
	private Integer fitness = 0;
	private int distance = 0;

	public Tour(List<City> array) {
		this.tour = array;
	}

	// Gets a city from the tour
	public City getCity(int tourPosition) {
		return (City) tour.get(tourPosition);
	}

	// ����С����Ӧֵ�ϴ�
	public Integer getFitness() {
		if (fitness == 0) {
			fitness = (1 / getDistance()) * 1000000;
		}
		return fitness;
	}

	// ��ȡ��ǰ·�����ܾ���
	public int getDistance() {
		if (distance == 0) {
			int tourDistance = 0;
			for (int cityIndex = 0; cityIndex < tourSize(); cityIndex++) {
				// Get city we're travelling from
				City fromCity = getCity(cityIndex);
				// City we're travelling to
				City destinationCity;
				// Check we're not on our tour's last city, if we are set our
				// tour's final destination city to our starting city
				if (cityIndex + 1 < tourSize()) {
					destinationCity = getCity(cityIndex + 1);
				} else {
					destinationCity = getCity(0);
				}
				// Get the distance between the two cities
				tourDistance += fromCity.distanceTo(destinationCity);
			}
			distance = tourDistance;
		}
		return distance;
	}

	// Get number of cities on our tour
	public int tourSize() {
		return tour.size();
	}

	// Check if the tour contains a city
	public boolean containsCity(City city) {
		return tour.contains(city);
	}

	@Override
	public String toString() {
		String geneString = "|";
		for (int i = 0; i < tourSize(); i++) {
			geneString += getCity(i) + "|";
		}
		return geneString;
	}
}